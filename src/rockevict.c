#include "server.h"
#include "dict.h"
#include "rockevict.h"
#include "rock.h"

#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255
#define EVICT_CANDIDATES_CACHED_SDS_SIZE 255

#define EVICT_TYPE_STRING   1
#define EVICT_TYPE_HASH     2

#define EVICT_HASH_CANDIDATES_MAX_SIZE  8

struct evictKeyPoolEntry {
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
    int dbid;                   /* Key DB number. */
};

static struct evictKeyPoolEntry *EvictKeyPool;

struct evictHashPoolEntry {
    unsigned long long idle;
    sds field;
    sds cached_field;     /* Cached SDS object for field. */
    sds key;
    sds cached_key;       /* Cached SDS object for key. */
    int dbid;
};

dictType fieldLruDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL,                       /* val destructor */
    dictExpandAllowed           /* allow to expand */
};

static struct evictHashPoolEntry *EvictHashPool;
/* used for cache the combined key for memory performance 
 * check combine_dbid_key() and free_combine_dbid_key() for more details */
static sds cached_combined_dbid_key = NULL;        

/* Create a new eviction pool for string. */
void evictKeyPoolAlloc(void) {
    struct evictKeyPoolEntry *ep;

    ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
    for (int j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    EvictKeyPool = ep;
}

/* Create a new eviction pool for hash. */
void evictHashPoolAlloc(void) {
    struct evictHashPoolEntry *ep;

    ep = zmalloc(sizeof(*ep)*EVPOOL_SIZE);
    for (int j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].field = NULL;
        ep[j].cached_field = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[j].cached_key = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    EvictHashPool = ep;
}

static void _dictRehashStep(dict *d) {
    if (d->pauserehash == 0) dictRehash(d,1);
}

/* We randomly select some keys which ty[e is 
 * if type == EVICT_TYPE_STRING: OBJ_STRING of OBJ_ENCODING_RAW or OBJ_ENCODING_EMBSTR. No shared object 
 * if type == EVICT_TYPE_HASH: OBJ_HASH of OBJ_ENCODING_HASH. No shared object 
 * not other evict type 
 * NOTE: if type == EVICT_TYPE_HASH, the d of dict is the hash dict: 
 *       key is sds string of field, values is sds string of val */
static unsigned int dictGetSomeKeysOfStringOrHashType(int evict_type,
                                                      dict *d, dict *lru, 
                                                      dictEntry **des, 
                                                      dictEntry **lru_des, 
                                                      unsigned int count) {   
    serverAssert(evict_type == EVICT_TYPE_STRING || evict_type == EVICT_TYPE_HASH);                                                

    if (dictSize(d) < count) count = dictSize(d);
    unsigned long maxsteps = count*10;

    unsigned long j; /* internal hash table id, 0 or 1. */
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

    unsigned long tables; /* 1 or 2 tables? */
    unsigned long stored = 0, maxsizemask;    
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
                    if (evict_type == EVICT_TYPE_STRING) {
                        sds key = dictGetKey(he);
                        lru_he = dictFind(lru, key);
                        serverAssert(lru_he);
                        /* Collect all the elements of the buckets found non
                        * empty while iterating. */
                        robj *o = he->v.val;
                        serverAssert(o);
                        if (o->type == OBJ_STRING && 
                            (o->encoding == OBJ_ENCODING_RAW || o->encoding == OBJ_ENCODING_EMBSTR) && 
                            o->refcount != OBJ_SHARED_REFCOUNT) {
                            *des = he;
                            *lru_des = lru_he;
                            des++;
                            lru_des++;
                            stored++;
                        }
                    } else {
                        sds field = dictGetKey(he);                        
                        lru_he = dictFind(lru, field);
                        serverAssert(lru_he);
                        sds val = dictGetVal(he);
                        if (val != shared.hashRockVal) {
                            *des = he;
                            *lru_des =lru_he;
                            des++;
                            lru_des++;
                            stored++;
                        }
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

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm. 
 * Refercnet evict.c estimateObjectIdleTime() for more details */
static unsigned long long estimateObjectIdleTimeFromLruDictEntry(dictEntry *de) {
    unsigned long long lruclock = LRU_CLOCK();
    unsigned long long my_lru = de->v.u64;
    if (lruclock >= my_lru) {
        return (lruclock - my_lru) * LRU_CLOCK_RESOLUTION;
    } else {
        return (lruclock + (LRU_CLOCK_MAX - my_lru)) *
                    LRU_CLOCK_RESOLUTION;
    }
}

/* NOTE: does not like evict.c similiar function, we do not evict anything from TTL dict */
static size_t evictKeyPoolPopulate(int dbid, dict *sampledict, dict *lru_dict, struct evictKeyPoolEntry *pool) {

    dictEntry *samples[server.maxmemory_samples];
    dictEntry *lru_samples[server.maxmemory_samples];
    int count = dictGetSomeKeysOfStringOrHashType(EVICT_TYPE_STRING, sampledict, lru_dict, samples, 
                                                  lru_samples, server.maxmemory_samples);

    size_t insert_cnt = 0;

    int j, k;
    unsigned long long idle;
    sds key;
    dictEntry *de;
    dictEntry *lru_de;
    sds cached;
    for (j = 0; j < count; j++) {
        de = samples[j];
        lru_de = lru_samples[j];
        key = dictGetKey(de);

        idle = estimateObjectIdleTimeFromLruDictEntry(lru_de);

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
                cached = pool[EVPOOL_SIZE-1].cached;
                memmove(pool+k+1,pool+k,
                    sizeof(pool[0])*(EVPOOL_SIZE-k-1));
                pool[k].cached = cached;
            } else {
                /* No free space on right? Insert at k-1 */
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time. */
                cached = pool[0].cached; /* Save SDS before overwriting. */
                if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                memmove(pool,pool+1,sizeof(pool[0])*k);
                pool[k].cached = cached;
            }
            ++insert_cnt;
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

    return insert_cnt;
}

/* NOTE1: does not like evict.c similiar function, we do not evict anything from TTL dict 
 * NOTE2: sampledict is the hash dict that key is sds of field and value is sds value (NOT robj*) */
static size_t evictHashPoolPopulate(int dbid, sds key, 
                                  dict *sampledict, dict *lru_dict, struct evictHashPoolEntry *pool) {

    dictEntry *samples[server.maxmemory_samples];
    dictEntry *lru_samples[server.maxmemory_samples];
    int count = dictGetSomeKeysOfStringOrHashType(EVICT_TYPE_HASH, sampledict, lru_dict, samples, 
                                                  lru_samples, server.maxmemory_samples);

    size_t insert_cnt = 0;

    int j, k;
    unsigned long long idle;
    sds field;
    // sds keyWithField;
    dictEntry *de;
    dictEntry *lru_de;
    sds cached_field;
    sds cached_key;
    for (j = 0; j < count; j++) {
        de = samples[j];
        lru_de = lru_samples[j];
        field = dictGetKey(de);

        idle = estimateObjectIdleTimeFromLruDictEntry(lru_de);

        /* Insert the element inside the pool.
         * First, find the first empty bucket or the first populated
         * bucket that has an idle time smaller than our idle time. */
        k = 0;
        while (k < EVPOOL_SIZE &&
               pool[k].field &&
               pool[k].idle < idle) k++;
        if (k == 0 && pool[EVPOOL_SIZE-1].field != NULL) {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            continue;
        } else if (k < EVPOOL_SIZE && pool[k].field == NULL) {
            /* Inserting into empty position. No setup needed before insert. */
        } else {
            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.  */
            if (pool[EVPOOL_SIZE-1].field == NULL) {
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */

                /* Save SDS before overwriting. */
                cached_field = pool[EVPOOL_SIZE-1].cached_field;
                cached_key = pool[EVPOOL_SIZE-1].cached_key;
                memmove(pool+k+1,pool+k,
                        sizeof(pool[0])*(EVPOOL_SIZE-k-1));
                pool[k].cached_field = cached_field;
                pool[k].cached_key = cached_key;
            } else {
                /* No free space on right? Insert at k-1 */
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time. */
                cached_field = pool[0].cached_field;
                cached_key = pool[0].cached_key; /* Save SDS before overwriting. */
                if (pool[0].field != pool[0].cached_field) sdsfree(pool[0].field);
                if (pool[0].key != pool[0].cached_key) sdsfree(pool[0].key);
                memmove(pool,pool+1,sizeof(pool[0])*k);
                pool[k].cached_field = cached_field;
                pool[k].cached_key = cached_key;
            }
            insert_cnt++;
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimization bla bla bla. */
        int field_klen = sdslen(field);
        if (field_klen > EVPOOL_CACHED_SDS_SIZE) {
            pool[k].field = sdsdup(field);
        } else {
            memcpy(pool[k].cached_field,field,field_klen+1);
            sdssetlen(pool[k].cached_field,field_klen);
            pool[k].field = pool[k].cached_field;
        }
        int key_klen = sdslen(key);
        if (key_klen > EVPOOL_CACHED_SDS_SIZE) {
            pool[k].key = sdsdup(key);
        } else {
            memcpy(pool[k].cached_key,key,key_klen+1);
            sdssetlen(pool[k].cached_key,key_klen);
            pool[k].key = pool[k].cached_key;
        }

        pool[k].idle = idle;
        pool[k].dbid = dbid;
    }

    return insert_cnt;
}

static int getRockKeyOfStringPercentage() {
    long long keyCnt = 0;
    long long rockCnt = 0;
    redisDb *db;

    for (int i = 0; i < server.dbnum; ++i) {
        db = server.db+i;
        keyCnt += db->stat_key_str_cnt;
        rockCnt += db->stat_key_str_rockval_cnt;
    }

    return keyCnt == 0 ? 100 :(int)((long long)100 * rockCnt / keyCnt);    
}

/* return EVICT_ROCK_OK if no need to eviction value or evict enough memory 
 * return EVICT_ROCK_PERCENTAGE if the percentage of string key with value in RocksDB of total string key is too high
 * return EVICT_FAIL_TIMEOUT if timeout and not enought memory has been released 
 * If must_od is ture, we ignore the memory over limit check */
int performKeyOfStringEvictions(int must_do, size_t must_tofree) {
    int keys_freed = 0;
    size_t mem_tofree;
    long long mem_freed; /* May be negative */
    long long delta;

    size_t used = zmalloc_used_memory();
    if (!must_do && used <= server.bunnymem)
        return EVICT_OK;        // used memory not over limit and not must_do

    // Check the percentage of RockKey vs total key. 
    // We do not need to waste time for high percentage of rock keys, e.g. 98%, 
    // because mostly it will be timeout for no evicting enough memory
    int rock_key_percentage = getRockKeyOfStringPercentage();
    if (rock_key_percentage >= ROCK_KEY_UPPER_PERCENTAGE) {
        return EVICT_ROCK_PERCENTAGE; 
    }

    if (must_do) {
        if (used >= server.bunnymem) {
            mem_tofree = used - server.bunnymem > must_tofree ? used - server.bunnymem : must_tofree;
        } else {
            mem_tofree = must_tofree;
        }
    } else {
        serverAssert(used > server.bunnymem);
        mem_tofree = used - server.bunnymem;
    }

    mem_freed = 0;
    int timeout = 0;
    monotime evictionTimer;
    elapsedStart(&evictionTimer);
    struct evictKeyPoolEntry *pool = EvictKeyPool;

    while (mem_freed < (long long)mem_tofree) {
        int k, i;
        sds bestkey = NULL;
        robj *valstrobj;
        int bestdbid;
        redisDb *db;
        dict *lru_dict;
        dict *dict;
        dictEntry *de;

        // struct evictKeyPoolEntry *pool = EvictKeyPool;

        int fail_cnt = 0;
        // unsigned long total_keys, keys;
        unsigned long total_keys;
        while(bestkey == NULL && fail_cnt < 100) {
            total_keys = 0;

            /* We don't want to make local-db choices when expiring keys,
             * so to start populate the eviction pool sampling keys from
             * every DB. */
            for (i = 0; i < server.dbnum; i++) {
                db = server.db+i;
                dict = db->dict;
                lru_dict = db->key_lrus;
                size_t rock_cnt = db->stat_key_str_rockval_cnt;

                size_t dict_cnt = dictSize(dict);
                if (dict_cnt != 0 && dict_cnt != rock_cnt) {
                    size_t insert_cnt = evictKeyPoolPopulate(i, dict, lru_dict, pool);
                    total_keys += insert_cnt;
                }
            }

            /* No insert keys and pool is empty skip evict. */
            if (total_keys == 0 && pool[0].key == NULL) break; 

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
                    if (valstrobj->type == OBJ_STRING && 
                        (valstrobj->encoding == OBJ_ENCODING_RAW || valstrobj->encoding == OBJ_ENCODING_EMBSTR) &&
                        valstrobj->refcount != OBJ_SHARED_REFCOUNT)
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

        if (elapsedMs(evictionTimer) >= 2) {        // at most 2-3 ms for eviction
            if (mem_freed < (long long)mem_tofree)
                timeout = 1;
            break;
        }
    }

    return timeout ? EVICT_ROCK_TIMEOUT : EVICT_ROCK_OK;
}

/* return EVICT_ROCK_OK if no need to eviction value or evict enough memory 
 * return EVICT_ROCK_PERCENTAGE if the percentage of hassh with value in RocksDB of total string key is too high
 * return EVICT_FAIL_TIMEOUT if timeout and not enought memory has been released 
 * If must_od is ture, we ignore the memory over limit check */
int performKeyOfHashEvictions(int must_do, size_t must_tofree) {
    int keys_freed = 0;
    size_t mem_tofree;
    long long mem_freed; /* May be negative */
    long long delta;

    size_t used = zmalloc_used_memory();
    if (!must_do && used <= server.bunnymem)
        return EVICT_OK;        // used memory not over limit and not must_do

    // Check the percentage of RockKey vs total key. 
    // We do not need to waste time for high percentage of rock keys, e.g. 98%, 
    // because mostly it will be timeout for no evicting enough memory
    /*
    int rock_key_percentage = getRockKeyOfStringPercentage();
    if (rock_key_percentage >= ROCK_KEY_UPPER_PERCENTAGE) {
        return EVICT_ROCK_PERCENTAGE; 
    }
    */

    if (must_do) {
        if (used >= server.bunnymem) {
            mem_tofree = used - server.bunnymem > must_tofree ? used - server.bunnymem : must_tofree;
        } else {
            mem_tofree = must_tofree;
        }
    } else {
        serverAssert(used > server.bunnymem);
        mem_tofree = used - server.bunnymem;
    }

    mem_freed = 0;
    int timeout = 0;
    monotime evictionTimer;
    elapsedStart(&evictionTimer);
    struct evictHashPoolEntry *pool = EvictHashPool;

    while (mem_freed < (long long)mem_tofree) {
        int k;
        sds bestfield = NULL;
        sds bestkeyWithField = NULL;
        int bestdbid = -1;
        sds valstrsds;
        // redisDb *db;
        // dict *lru_dict;

        // dict *dict_of_db;
        dictEntry *de_of_db;
        dict *dict_of_hash;
        dictEntry *de_of_hash;

        int fail_cnt = 0;
        // unsigned long total_keys, keys;
        unsigned long total_keys;
        while(bestfield == NULL && fail_cnt < 100) {
            total_keys = 0;

            /* We don't want to make local-db choices when expiring fields,
             * so to start populate the eviction pool sampling keys from
             * every evict_hash_candidates for all dbs. */
            dictIterator *di_candidates;
            dictEntry *de_candidates;
            di_candidates = dictGetIterator(server.evict_hash_candidates);
            while ((de_candidates = dictNext(di_candidates))) {
                // sds dbid_with_key = dictGetKey(de_candidates);
                // serverAssert(dbid_with_key && sdslen(dbid_with_key) > 1);
                // uint8_t dbid = dbid_with_key[0];
                // serverAssert(dbid < server.dbnum);
                evictHash *evict_hash = dictGetVal(de_candidates);
                serverAssert(evict_hash);
                sds combined = dictGetKey(de_candidates);

                // sds db_hash_key = evict_hasss_lru_and_rock_stat->key;
                // dictEntry *de_in_db = dictFind(server.db[dbid].dict, db_hash_key);
                // serverAssert(de_in_db);
                // dict *sample_dict = dictGetVal(de_in_db);                 
                
                uint8_t dbid = combined[0];
                sds db_key = evict_hash->key;
                dict *sample_dict = evict_hash->dict_hash;
                dict *field_lru = evict_hash->field_lru;
                size_t rock_cur_cnt = evict_hash->rock_cnt;

                size_t sample_cnt = dictSize(sample_dict);
                if (sample_cnt != 0 && sample_cnt != rock_cur_cnt) {
                    size_t inserted_cnt = evictHashPoolPopulate(dbid, db_key, sample_dict, field_lru, pool);
                    total_keys += inserted_cnt;
                }
            }
            dictReleaseIterator(di_candidates);

            /* No insert keys and pool is empty, skip to evict. */
            if (total_keys == 0 && pool[0].field == NULL) break; 

            /* Go backward from best to worst element to evict. */
            for (k = EVPOOL_SIZE-1; k >= 0; k--) {
                if (pool[k].field == NULL) {
                    bestfield = NULL;
                    continue;
                }

                bestdbid = pool[k].dbid;
                bestfield = pool[k].field;
                bestkeyWithField = pool[k].key;

                de_of_hash = NULL;
                de_of_db = dictFind(server.db[bestdbid].dict, bestkeyWithField);
                if (de_of_db) {
                    robj *o = dictGetVal(de_of_db);
                    if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT) {
                        dict_of_hash = o->ptr;
                        de_of_hash = dictFind(dict_of_hash, bestfield);
                    }
                }

                /* Remove the entry from the pool. */
                if (pool[k].field != pool[k].cached_field)
                    sdsfree(pool[k].field);
                if (pool[k].key != pool[k].cached_key)
                    sdsfree(pool[k].key);
                pool[k].field = NULL;
                pool[k].key = NULL;
                pool[k].dbid = -1;
                pool[k].idle = 0;

                /* If the key exists, is our pick. Otherwise it is
                 * a ghost and we need to try the next element. */
                /* What is the situation for ghost? */
                /* The pool's element has previous value which could be changed, */
                /*     e.g. DEL and change anotther type */
                /* These kinds of elements in the pool  which are not string type */
                /*     or value has been saved to Rocksdb are Ghost */
                if (de_of_hash) {
                    valstrsds = (sds)dictGetVal(de_of_hash);
                    if (valstrsds == shared.hashRockVal) 
                        bestfield = NULL;   // ghost
                } else {
                    bestfield = NULL;   // ghost
                }
                if (bestfield) break;
            }
            if (bestfield == NULL) ++fail_cnt;
        }

        /* Finally convert value of the selected key and write it to RocksDB write queue */
        if (bestfield) {
            delta = (long long) zmalloc_used_memory();
            dictSetVal(dict_of_hash, de_of_hash, shared.hashRockVal);
            // update stat if possible
            evictHash *evict_hash = lookupEvictOfHash(bestdbid, bestkeyWithField);
            if (evict_hash) {
                ++evict_hash->rock_cnt;
            }
            // addRockWriteTaskOfString(bestdbid, bestkey, valstrobj->ptr);
            sdsfree(valstrsds);
            delta -= (long long) zmalloc_used_memory();
            mem_freed += delta;
            keys_freed++;
        } 

        if (elapsedMs(evictionTimer) >= 2) {        // at most 2-3 ms for eviction
            if (mem_freed < (long long)mem_tofree)
                timeout = 1;
            break;
        }
    }

    return timeout ? EVICT_ROCK_TIMEOUT : EVICT_ROCK_OK;
}


static void debugReportMemAndKey() {
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
            serverLog(LL_WARNING, 
                      "dbid = %d, keyCnt = %d, strCnt = %d, rockCnt = %d, stat_key_str_ cnt = %lld, stat_key_str_rockval_cnt = %lld", 
                      i, keyCnt, strCnt, rockCnt, 
                      (server.db+i)->stat_key_str_cnt, (server.db+i)->stat_key_str_rockval_cnt);
    }
}

static void debugReportMemAndHash() {
    size_t used = zmalloc_used_memory();
    size_t mem_tofree = 0;
    if (used > server.bunnymem)
        mem_tofree = used - server.bunnymem;

    serverLog(LL_WARNING, "to free = %zu, used = %zu, bunnymem = %llu", mem_tofree, used, server.bunnymem);

    dictIterator *di;
    dictEntry *de;
    di = dictGetIterator(server.evict_hash_candidates);
    while ((de = dictNext(di))) {
        evictHash *evict_hash = dictGetVal(de);
        sds combined = dictGetKey(de);

        uint8_t dbid = combined[0];
        sds key = evict_hash->key;
        long long rock_cnt = evict_hash->rock_cnt;
        dict *dict_hash = evict_hash->dict_hash;
        dict *dict_lru = evict_hash->field_lru;

        serverLog(LL_WARNING, "hash candidates, dbid = %u, key = %s, field cnt = %lu, lru cnt = %lu, rock cnt = %lld",
            dbid, key, dictSize(dict_hash), dictSize(dict_lru), rock_cnt);
    }
    dictReleaseIterator(di);
}

void debugEvictCommand(client *c) {
    sds flag = c->argv[1]->ptr;

    if (strcasecmp(flag, "evictkey") == 0) {
        serverLog(LL_WARNING, "debugEvictCommand() for key has been called!");
        serverLog(LL_WARNING, "===== before evcition ===========");
        debugReportMemAndKey();
        serverLog(LL_WARNING, "===== after evcition ===========");
        int res = performKeyOfStringEvictions(0, 0);
        serverLog(LL_WARNING, "performKeyOfStringEvictions() res = %s", 
            res == EVICT_ROCK_OK ? "EVICT_ROCK_OK" : 
                                   (res == EVICT_ROCK_PERCENTAGE ? "EVICT_ROCK_PERCENTAGE" : "EVICT_ROCK_TIMEOUT"));
        debugReportMemAndKey();
    } else if (strcasecmp(flag, "evicthash") == 0) {
        serverLog(LL_WARNING, "debugEvictCommand() for hash has been called!");
        serverLog(LL_WARNING, "===== before evcition ===========");
        debugReportMemAndHash();
        serverLog(LL_WARNING, "===== after evcition ===========");
        int res = performKeyOfHashEvictions(0, 0);
        serverLog(LL_WARNING, "performKeyOfHashEvictions() res = %s", 
            res == EVICT_ROCK_OK ? "EVICT_ROCK_OK" : 
                                   (res == EVICT_ROCK_PERCENTAGE ? "EVICT_ROCK_PERCENTAGE" : "EVICT_ROCK_TIMEOUT"));
        debugReportMemAndHash();
    } else if (strcasecmp(flag, "reportkey") == 0) {
        debugReportMemAndKey();
    } else if (strcasecmp(flag, "reporthash") == 0) {
        debugReportMemAndHash();
    } else {
        addReplyError(c, "wrong flag for debugevict!");
        return;
    }

    addReplyBulk(c,c->argv[0]);
}

/* In networking.c processInputBuffer(), we need to check the memory usage */
/* If the memory is over limit and the command would increase memroy, we return C_ERR */
/* otherwise, incluing the READ_ONLY command, we return C_OK */
/* NOTE: we can only use c->argv but not c->cmd, but the c->argv[0] guarantee to be valid */
/* please referecne server.c processCommand(), we use some similiar code but we can not */
/* call processCommand() because processCommand() is guaranteed to be passed through all checks */
int checkMemInProcessBuffer(client *c) {
    serverAssert(c->argc > 0);

    int out_of_memory = (zmalloc_used_memory() > server.bunnymem);
    if (!out_of_memory) return C_OK;        // memory not over limit

    if (strcasecmp(c->argv[0]->ptr, "quit") == 0) return C_OK;

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

/* cron job to make some room to avoid the forbidden command due to memory limit */
#define ENOUGH_MEM_SPACE 50<<20         // if we have enought free memory of 50M, do not need to evict
void cronEvictToMakeRoom() {
    // debug 
    return;

    size_t used_mem = zmalloc_used_memory();
    size_t limit_mem = server.bunnymem;

    if (limit_mem > used_mem) {
        if (limit_mem - used_mem >= ENOUGH_MEM_SPACE) return;    
    }

    if (used_mem * 1000 / limit_mem <= 950) return;       // only over 95%, we start to evict

    int res = performKeyOfStringEvictions(1, 1<<20);      // evict at least 1M bytes, server.hz set 50, so 1 second call 50 times

    static int over_pencentage_cnt = 0;
    if (res == EVICT_ROCK_PERCENTAGE) {
        ++over_pencentage_cnt;
        over_pencentage_cnt = over_pencentage_cnt % 128;        // if Hz 50, so 2 seconds report once
        if (over_pencentage_cnt == 0)
            serverLog(LL_WARNING, "getRockKeyOfStringPercentage() over limit of percentage = %d%%", ROCK_KEY_UPPER_PERCENTAGE);
    }
}

/* This will allocate a new resource for combined key of dbid and key.
 * The caller needs to sdsfree() the resource */
sds combine_dbid_key_by_alloc(const uint8_t dbid, const sds key) {
    sds combined;
        
    combined = sdsnewlen(&dbid, sizeof(uint8_t));
    combined = sdscatlen(combined, key, sdslen(key));

    return combined;
}

/* combined dbid and key. It is for hash type eviction usage 
 * NOTE: It may use new allocated resource or just cached resource.
 *       The caller must call free_combine_dbid_key() to deal with the resource */
sds combine_dbid_key(const uint8_t dbid, const sds key) {
    if (cached_combined_dbid_key == NULL)
        cached_combined_dbid_key = sdsnewlen(NULL, EVICT_CANDIDATES_CACHED_SDS_SIZE);

    size_t key_len =  sdslen(key);
    if (sizeof(uint8_t) + key_len <= EVICT_CANDIDATES_CACHED_SDS_SIZE) {
        // use cached memory
        cached_combined_dbid_key[0] = dbid;
        memcpy(cached_combined_dbid_key+1, key, key_len+1);
        sdssetlen(cached_combined_dbid_key, 1+key_len);

        return cached_combined_dbid_key;
    } else {
        // use allocated memory
        return combine_dbid_key_by_alloc(dbid, key);
    }
}

void free_combine_dbid_key(sds to_free) {
    if (to_free != cached_combined_dbid_key)
        sdsfree(to_free);
}

/* find evictHash in server.evict_hash_candidates 
 * If not found return NULL. We use cached for memory because hash evict call it frequently */
evictHash* lookupEvictOfHash(uint8_t dbid, sds key) {
    sds combined = combine_dbid_key(dbid, key);
    dictEntry* de = dictFind(server.evict_hash_candidates, combined);
    free_combine_dbid_key(combined);

    evictHash *evict_hash = NULL;
    if (de) {
        evict_hash = dictGetVal(de);
        serverAssert(evict_hash);
        // for debug check
        serverAssert(dictSize(evict_hash->dict_hash) == dictSize(evict_hash->field_lru));
    }
    return evict_hash;
}

static dictEntry* findSmallestHashCandidate() {
    // We only check when server.evict_hash_candidates is full
    serverAssert(dictSize(server.evict_hash_candidates) == EVICT_HASH_CANDIDATES_MAX_SIZE);

    dictEntry *smallest = NULL;
    size_t smallest_sz = UINT64_MAX;
    dictIterator *di = dictGetIterator(server.evict_hash_candidates);
    dictEntry *de;
    while ((de = dictNext(di))) {
        evictHash *evict_hash = dictGetVal(de);
        size_t current_sz = dictSize(evict_hash->dict_hash);
        if (current_sz < smallest_sz) {
            smallest_sz = current_sz;
            smallest = de;
        }
    }
    dictReleaseIterator(di);
    return smallest;
}

/* Success remove return 1. otherwise return 0 
 * reference constructInserted() to see how release memory 
 * NOTE: 
 *      if input parameter combined_key is the same of the internal, does not free. The caller need to do this */
int removeHashCandidate(sds combined_key) {
    dictEntry *de = dictFind(server.evict_hash_candidates, combined_key);
    if (!de) 
        return 0;

    // clear memory
    evictHash *evict_hash = dictGetVal(de);
    sds internal_combined = dictGetKey(de);

    serverLog(LL_WARNING, "removeHashCandidate for key = %s", evict_hash->key);

    // key in field_lru does not need to free because the owner is the hash dict itself, i.e., evict_hash->dict
    dictRelease(evict_hash->field_lru);    
    sdsfree(evict_hash->key);
    zfree(evict_hash);

    // the matched internal combined_key as key of evict_hash_candidates can not be freed by itself 
    // becasue internal combined key may be the same address as combined_key which can leed corruption. 
    // Check dict.c dictGenericDelete() and server.c evictHashCandidatesDictType for more details
    dictDelete(server.evict_hash_candidates, combined_key);
    if (combined_key != internal_combined)
        sdsfree(internal_combined);    

    return 1; 
}

/* allocate an evictHash for inserted. dict is the hash dictionary */
static evictHash* constructInserted(const sds key, dict *dict) {
    evictHash *inserted = zmalloc(sizeof(*inserted));
    inserted->key = sdsdup(key);
    inserted->dict_hash = dict;

    long long rock_cnt = 0;
    inserted->field_lru = dictCreate(&fieldLruDictType, NULL);
    dictIterator *di = dictGetIterator(dict);
    dictEntry *de;
    while((de = dictNext(di))) {
        sds val = dictGetVal(de);
        if (val == shared.hashRockVal) 
            ++rock_cnt;

        sds field = dictGetKey(de);     // we share the same sds object of field with the hash dict
        uint64_t clock = LRU_CLOCK();
        // initialize lru is right now for every field, so they may be a little different (good for eviction pick)
        dictAdd(inserted->field_lru, field, (void*)clock);  
    }
    inserted->rock_cnt = rock_cnt;
    dictReleaseIterator(di);

    return inserted;
}

static int addToHashCandidatesIfPossible(const uint8_t dbid, const sds key, dict *dict) {
    // NOTE: combined_key can not use cached memory, because it will be free by removeHashCandidate() 
    sds combined_key = combine_dbid_key_by_alloc(dbid, key);   
    serverAssert(dictFind(server.evict_hash_candidates, combined_key) == NULL);

    if (dictSize(server.evict_hash_candidates) < EVICT_HASH_CANDIDATES_MAX_SIZE) {
        // there is room, just inserted
        evictHash *inserted = constructInserted(key, dict);
        dictAdd(server.evict_hash_candidates, combined_key, inserted);
        return 1;
    } else {
        // no room
        dictEntry *smallest = findSmallestHashCandidate();
        serverAssert(smallest);
        sds smallest_combined = dictGetKey(smallest);
        evictHash *smallest_evic_hash = dictGetVal(smallest);
        if (dictSize(smallest_evic_hash->dict_hash) >= dictSize(dict)) {
            // no need to insert because the smallest in server.evict_hash_candidates has more fields
            sdsfree(combined_key);
            return 0;
        } else {
            int ret = removeHashCandidate(smallest_combined);
            sdsfree(smallest_combined);     // because it is from the interal of smallests
            serverAssert(ret);
            evictHash *inserted = constructInserted(key, dict);
            dictAdd(server.evict_hash_candidates, combined_key, inserted);
            return 1;
        }
    }
}

/* We check whether need to add to server.evict_hash_candiddates 
 * check is from getEvictOfHash() by the caller. 
 * Because server.evict_hash_candiddates has a bounded size, i.e., EVICT_HASH_CANDIDATES_MAX_SIZE
 * and insert or remove is expensive,
 * we check for an interval when added is just over a CHECK_INTERVAL. 
 * For example, if we set CHECK_INTERVAL == 1000, when dict is added, we see the following results
 *     dict size == 1000, added = 1. check candidates
 *     dict size == 1001, added = 1. no check
 *     dict size == 2001, added = 2. check candidates
 *     dict size == 1999, added = 999. no check 
 * RETURN: if added to candidates, return 1. otherwise 0 */
#define CHECK_INTERVAL 128
int checkAddToEvictHashCandidates(const uint8_t dbid, const size_t added, sds key, dict *dict) {
    if (added == 0) return 0;
    
    size_t now = dictSize(dict);
    serverAssert(now >= added);
    size_t before = now - added;

    if (now/CHECK_INTERVAL != before/CHECK_INTERVAL) {
        return addToHashCandidatesIfPossible(dbid, key, dict);
    } else {
        return 0;   // now added for over CHECK_INTERVAL, no check
    }    
}

/* because flush db use simple remove dict code, we need to clear all resource relating to rock */
void clearEvictByEmptyDb(uint8_t dbid) {
    serverAssert(dbid < server.dbnum);

    // clear rock stat and lru for such db
    // but we do not need do anything becuase stat and key_lrus are in struct of redisDb
    // check dictEmpty() and _dictClear() for more details

    // find the candidates in evict_hash_candidates
    while (1) {
        sds found = NULL;

        dictIterator *di;
        dictEntry *de;
        di = dictGetIterator(server.evict_hash_candidates);
        while ((de = dictNext(di))) {
            sds combined = dictGetKey(de);
            if (combined[0] == dbid) {
                found = combined;
                break;
            }
        }
        dictReleaseIterator(di);

        if (found) {
            removeHashCandidate(found);
            sdsfree(found);     // because found is the internal combined key
        } else {
            break;
        }
    }
}

/* the object is the obj in db->dict */
void updateHashLru(uint8_t dbid, sds key, sds field) {
    redisDb *db = server.db+dbid;
    dictEntry *de = dictFind(db->dict, key);
    if (!de) return;
    
    robj *o = dictGetVal(de);
    evictHash* evict_hash 
        = o->encoding == OBJ_ENCODING_HT ? lookupEvictOfHash(dbid, key) : NULL;
    if (evict_hash) {
        dict *lru = evict_hash->field_lru;
        dictEntry *de = dictFind(lru, field);
        if (de) {
            // serverLog(LL_WARNING, "update hash lru for key = %s, field = %s", key, field);
            uint64_t clock = LRU_CLOCK();
            dictGetVal(de) = (void *)clock;
        }
    }
}

/* field maybe in lru or not */
void setForHashLru(uint8_t dbid, sds key, sds field) {
    redisDb *db = server.db+dbid;
    dictEntry *de = dictFind(db->dict, key);
    if (!de) return;

    robj *o = dictGetVal(de);
    evictHash* evict_hash 
        = o->encoding == OBJ_ENCODING_HT ? lookupEvictOfHash(dbid, key) : NULL;
    if (evict_hash) {
        dict *lru = evict_hash->field_lru;
        uint64_t clock = LRU_CLOCK();
        dictEntry *de = dictFind(lru, field);
        if (!de) {
            // if field not exists, insert to lru
            dictAdd(lru, field, (void *)clock);
        } else {
            // only need to update the lru for existed field
            dictGetVal(de) = (void *)clock;
        }
    }
}

/* update all lru for the dbid if in candiates */
void updateHashLruForKey(uint8_t dbid, sds key) {
    redisDb *db = server.db+dbid;
    dictEntry *de_in_db = dictFind(db->dict, key);
    if (!de_in_db) return;

    robj *o = dictGetVal(de_in_db);
    evictHash* evict_hash 
        = o->encoding == OBJ_ENCODING_HT ? lookupEvictOfHash(dbid, key) : NULL;
    if (evict_hash) {
        uint64_t clock = LRU_CLOCK();
        dict *lru = evict_hash->field_lru;
        dictIterator *di;
        dictEntry *de;
        di = dictGetIterator(lru);
        while ((de = dictNext(di))) {
            // sds field = dictGetKey(de);
            // serverLog(LL_WARNING, "update hash lru (all) for key = %s, field = %s", key, field);
            dictGetVal(de) = (void *)clock;
        }
        dictReleaseIterator(di);
    }
}