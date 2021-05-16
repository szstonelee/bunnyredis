#include "server.h"
#include "dict.h"
#include "rockevict.h"
#include "rock.h"

#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255
struct evictKeyPoolEntry {
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
    int dbid;                   /* Key DB number. */
};

static struct evictKeyPoolEntry *EvictKeyPool;

/* Create a new eviction pool. */
void evictKeyPoolAlloc(void) {
    struct evictKeyPoolEntry *ep;
    int j;

    ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    EvictKeyPool = ep;
}

static void _dictRehashStep(dict *d) {
    if (d->pauserehash == 0) dictRehash(d,1);
}

static unsigned int dictGetSomeKeysOfStringType(dict *d, dict *lru, dictEntry **des, dictEntry **lru_des, unsigned int count) {
    unsigned long j; /* internal hash table id, 0 or 1. */
    unsigned long tables; /* 1 or 2 tables? */
    unsigned long stored = 0, maxsizemask;
    unsigned long maxsteps;

    if (dictSize(d) < count) count = dictSize(d);
    maxsteps = count*10;

    /* Try to do a rehashing work proportional to 'count'. */
    for (j = 0; j < count; j++) {
        if (dictIsRehashing(d))
            _dictRehashStep(d);
        else
            break;
    }
    for (j = 0; j < count; j++) {
        if (dictIsRehashing(lru))
            _dictRehashStep(lru);
        else
            break;
    }

    tables = dictIsRehashing(d) ? 2 : 1;
    maxsizemask = d->ht[0].sizemask;
    if (tables > 1 && maxsizemask < d->ht[1].sizemask)
        maxsizemask = d->ht[1].sizemask;

    /* Pick a random point inside the larger table. */
    unsigned long i = randomULong() & maxsizemask;
    unsigned long emptylen = 0; /* Continuous empty entries so far. */
    while(stored < count && maxsteps--) {
        for (j = 0; j < tables; j++) {
            /* Invariant of the dict.c rehashing: up to the indexes already
             * visited in ht[0] during the rehashing, there are no populated
             * buckets, so we can skip ht[0] for indexes between 0 and idx-1. */
            if (tables == 2 && j == 0 && i < (unsigned long) d->rehashidx) {
                /* Moreover, if we are currently out of range in the second
                 * table, there will be no elements in both tables up to
                 * the current rehashing index, so we jump if possible.
                 * (this happens when going from big to small table). */
                if (i >= d->ht[1].size)
                    i = d->rehashidx;
                else
                    continue;
            }
            if (i >= d->ht[j].size) continue; /* Out of range for this table. */
            dictEntry *he = d->ht[j].table[i];

            /* Count contiguous valid buckets, and jump to other
             * locations if they reach 'count' (with a minimum of 5). */
            if (he == NULL) {
                emptylen++;
                if (emptylen >= 5 && emptylen > count) {
                    i = randomULong() & maxsizemask;
                    emptylen = 0;
                }
            } else {
                emptylen = 0;
                dictEntry *lru_he;
                while (he) {
                    sds key = dictGetKey(he);
                    lru_he = dictFind(lru, key);
                    serverAssert(lru_he);
                    /* Collect all the elements of the buckets found non
                     * empty while iterating. */
                    robj *o = he->v.val;
                    serverAssert(o);
                    if (o->type == OBJ_STRING && o->refcount != OBJ_SHARED_REFCOUNT) {
                        *des = he;
                        *lru_des = lru_he;
                        des++;
                        lru_des++;
                        stored++;
                    }

                    he = he->next;
                    if (stored == count) return stored;
                }
            }
        }
        i = (i+1) & maxsizemask;
    }
    return stored;
}

/* NOTE: does not like evict.c similiar function, we do not evict anything from TTL dict */
static void evictKeyPoolPopulate(int dbid, dict *sampledict, dict *lru_dict, struct evictKeyPoolEntry *pool) {
    int j, k, count;
    dictEntry *samples[server.maxmemory_samples];
    dictEntry *lru_samples[server.maxmemory_samples];

    count = dictGetSomeKeysOfStringType(sampledict, lru_dict, samples, lru_samples, server.maxmemory_samples);

    for (j = 0; j < count; j++) {
        unsigned long long idle;
        sds key;
        // robj *o;
        dictEntry *de;
        dictEntry *lru_de;

        de = samples[j];
        lru_de = lru_samples[j];
        key = dictGetKey(de);

        // o = dictGetVal(de);
        // idle = estimateObjectIdleTime(o);
        idle = dictGetUnsignedIntegerVal(lru_de);

        /* Insert the element inside the pool.
         * First, find the first empty bucket or the first populated
         * bucket that has an idle time smaller than our idle time. */
        k = 0;
        while (k < EVPOOL_SIZE &&
               pool[k].key &&
               pool[k].idle < idle) k++;
        if (k == 0 && pool[EVPOOL_SIZE-1].key != NULL) {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            continue;
        } else if (k < EVPOOL_SIZE && pool[k].key == NULL) {
            /* Inserting into empty position. No setup needed before insert. */
        } else {
            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.  */
            if (pool[EVPOOL_SIZE-1].key == NULL) {
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */

                /* Save SDS before overwriting. */
                sds cached = pool[EVPOOL_SIZE-1].cached;
                memmove(pool+k+1,pool+k,
                    sizeof(pool[0])*(EVPOOL_SIZE-k-1));
                pool[k].cached = cached;
            } else {
                /* No free space on right? Insert at k-1 */
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time. */
                sds cached = pool[0].cached; /* Save SDS before overwriting. */
                if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                memmove(pool,pool+1,sizeof(pool[0])*k);
                pool[k].cached = cached;
            }
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimization bla bla bla. */
        int klen = sdslen(key);
        if (klen > EVPOOL_CACHED_SDS_SIZE) {
            pool[k].key = sdsdup(key);
        } else {
            memcpy(pool[k].cached,key,klen+1);
            sdssetlen(pool[k].cached,klen);
            pool[k].key = pool[k].cached;
        }
        pool[k].idle = idle;
        pool[k].dbid = dbid;
    }
}

static int getKeyOfStringPercentage() {
    long long keyCnt = 0;
    long long rockCnt = 0;
    redisDb *db;

    for (int i = 0; i < server.dbnum; ++i) {
        db = server.db+i;
        keyCnt += db->stat_key_str_cnt;
        rockCnt += db->stat_key_str_rockval_cnt;
    }

    return (int)((long long)100 * rockCnt / keyCnt);
}

/* return C_OK if no need to eviction value or evict enough value 
 * return C_ERR 
 *     1. if timeout and not enought memory has been released 
 *     2. the percentage of string key with value in RocksDB of total string key is too high */  
int performKeyOfStringEvictions(void) {
    int keys_freed = 0;
    size_t mem_tofree;
    long long mem_freed; /* May be negative */
    long long delta;

    size_t used = zmalloc_used_memory();
    if (used <= server.bunnymem)
        return EVICT_OK;        // used memory not over limit

    // check percentage
    int percentage = getKeyOfStringPercentage();
    if (percentage >= 98) return C_ERR;     // we do not need to waste time for high percentage of rock keys
    
    mem_tofree = used - server.bunnymem;
    mem_freed = 0;
    int timeout = 0;
    monotime evictionTimer;
    elapsedStart(&evictionTimer);

    while (mem_freed < (long long)mem_tofree) {
        int k, i;
        sds bestkey = NULL;
        robj *valstrobj;
        int bestdbid;
        redisDb *db;
        dict *lru_dict;
        dict *dict;
        dictEntry *de;

        struct evictKeyPoolEntry *pool = EvictKeyPool;

        int fail_cnt = 0;
        while(bestkey == NULL && fail_cnt < 100) {
            unsigned long total_keys = 0, keys;

            /* We don't want to make local-db choices when expiring keys,
             * so to start populate the eviction pool sampling keys from
             * every DB. */
            for (i = 0; i < server.dbnum; i++) {
                db = server.db+i;
                dict = db->dict;
                lru_dict = db->key_lrus;

                if ((keys = dictSize(dict)) != 0) {
                    evictKeyPoolPopulate(i, dict, lru_dict, pool);
                    total_keys += keys;
                }
            }
            if (!total_keys) break; /* No keys to evict. */

            /* Go backward from best to worst element to evict. */
            for (k = EVPOOL_SIZE-1; k >= 0; k--) {
                if (pool[k].key == NULL) continue;
                bestdbid = pool[k].dbid;

                dict = server.db[pool[k].dbid].dict;
                de = dictFind(dict, pool[k].key);

                /* Remove the entry from the pool. */
                if (pool[k].key != pool[k].cached)
                    sdsfree(pool[k].key);
                pool[k].key = NULL;
                pool[k].idle = 0;

                /* If the key exists, is our pick. Otherwise it is
                 * a ghost and we need to try the next element. */
                /* What is the situation for ghost? */
                /* The pool's element has previous value which could be changed, */
                /*     e.g. DEL and change anotther type */
                /* These kinds of elements in the pool  which are not string type */
                /*     or value has been saved to Rocksdb are Ghost */
                int is_ghost = 1;
                if (de) {
                    valstrobj = (robj*)dictGetVal(de);
                    if (valstrobj->type == OBJ_STRING && valstrobj->refcount != OBJ_SHARED_REFCOUNT)
                        is_ghost = 0;                       
                }
                if (!is_ghost) {
                    bestkey = dictGetKey(de);
                    break;
                }
            }
            if (bestkey == NULL) ++fail_cnt;
        }

        /* Finally convert value of the selected key and write it to RocksDB write queue */
        if (bestkey) {
            db = server.db+bestdbid;
            delta = (long long) zmalloc_used_memory();
            dictSetVal(dict, de, shared.keyRockVal);
            ++db->stat_key_str_rockval_cnt;
            addRockWriteTaskOfString(bestdbid, bestkey, valstrobj->ptr);
            decrRefCount(valstrobj);
            delta -= (long long) zmalloc_used_memory();
            mem_freed += delta;
            keys_freed++;
        } 

        if (elapsedMs(evictionTimer) >= 5) {
            if (mem_freed < (long long)mem_tofree)
                timeout = 1;
            break;
        }
    }

    return timeout ? C_ERR : C_OK;
}

void debugReportMemAndKey() {
    size_t used = zmalloc_used_memory();
    size_t mem_tofree = 0;
    if (used > server.bunnymem)
        mem_tofree = used - server.bunnymem;

    serverLog(LL_WARNING, "to free = %zu, used = %zu, bunnymem = %llu", mem_tofree, used, server.bunnymem);

    dictIterator *di;
    dictEntry *de;
    for (int i = 0; i < server.dbnum; ++i) {
        int keyCnt = 0;
        int strCnt = 0;
        int rockCnt = 0;

        di = dictGetIterator(server.db[i].dict);
        while ((de = dictNext(di))) {
            ++keyCnt;
            robj *o = dictGetVal(de);
            if (o->type == OBJ_STRING)
            {
                ++strCnt;
                if (o == shared.keyRockVal)
                    ++rockCnt;
            }        
        }
        dictReleaseIterator(di);

        if (keyCnt)
            serverLog(LL_WARNING, "dbid = %d, keyCnt = %d, strCnt = %d, rockCnt = %d", i, keyCnt, strCnt, rockCnt);
    }
}

void debugEvictCommand(client *c) {
    serverLog(LL_WARNING, "debugEvictCommand() has been called!");

    serverLog(LL_WARNING, "===== before evcition ===========");
    debugReportMemAndKey();
    serverLog(LL_WARNING, "===== after evcition ===========");
    int res = performKeyOfStringEvictions();
    serverLog(LL_WARNING, "performKeyOfStringEvictions() res = %s", res == C_OK ? "C_OK" : "C_ERR");
    debugReportMemAndKey();

    addReplyBulk(c,c->argv[0]);
}

/* In networking.c processInputBuffer(), we need to check the memory usage */
/* If the memory is over limit and the command would increase memroy, we return C_ERR */
/* otherwise, incluing the READ_ONLY command, we return C_OK */
/* NOTE: we can only use c->argv but not c->cmd, but the c->argv[0] guarantee to be valid */
/* please referecne server.c processCommand(), we use some similiar code but we can not */
/* call processCommand() because processCommand() is guaranteed to be passed through all checks */
int checkMemInProcessBuffer(client *c) {
    return C_OK;        // for test

    serverAssert(c->argc > 0);

    int out_of_memory = (zmalloc_used_memory() > server.bunnymem);
    if (!out_of_memory) return C_OK;        // memory not over limit

    if (strcasecmp(c->argv[0]->ptr,"quit") == 0) return C_OK;

    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);
    if (!cmd) return C_OK;

    int is_denyoom_command = (cmd->flags & CMD_DENYOOM) ||
                             (cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_DENYOOM));

    int reject_cmd_on_oom = is_denyoom_command;
    /* If client is in MULTI/EXEC context, queuing may consume an unlimited
     * amount of memory, so we want to stop that.
     * However, we never want to reject DISCARD, or even EXEC (unless it
     * contains denied commands, in which case is_denyoom_command is already
     * set. */
    if (c->flags & CLIENT_MULTI &&
        cmd->proc != execCommand &&
        cmd->proc != discardCommand &&
        cmd->proc != resetCommand) {
        reject_cmd_on_oom = 1;
    }
    
    return reject_cmd_on_oom ? C_ERR : C_OK;
}