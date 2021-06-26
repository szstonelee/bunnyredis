#include "server.h"
#include "dict.h"
#include "rockevict.h"
#include "rock.h"
#include "streamwrite.h"

#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255
#define EVICT_CANDIDATES_CACHED_SDS_SIZE 255

#define EVICT_TYPE_STRING_OR_ZIPLIST    1
#define EVICT_TYPE_PURE_HASH            2


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

dictType pureHashNoRockDictType = {
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
// static sds cached_combined_dbid_key = NULL;        

/* Create a new eviction pool for string or ziplist */
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

/* Create a new eviction pool for pure hash. */
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

/*
static int debug_check_evict_hash(evictHash *evict_hash) {
    serverAssert(evict_hash->used);

    dict *dict_hash = evict_hash->dict_hash;
    dict *dict_lru = evict_hash->field_lru;
    uint8_t dbid = evict_hash->dbid;
    sds key = evict_hash->key;
    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, key);
    serverAssert(de_db);
    robj *o = dictGetVal(de_db);
    serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT && o->refcount != OBJ_SHARED_REFCOUNT);
    serverAssert(dict_hash == o->ptr);
    serverAssert(dictSize(dict_hash) == dictSize(dict_lru));
    
    dictIterator *di = dictGetIterator(dict_hash);
    dictEntry* de;
    int not_match_cnt = 0;
    while((de = dictNext(di))) {
        sds field_in_hash = dictGetKey(de);
        dictEntry *check_in_lru = dictFind(dict_lru, field_in_hash);
        if(check_in_lru == NULL) {
            serverLog(LL_WARNING, "dictFind(dict_lru, field_in_hash) failed, field_in_hash = %s", field_in_hash);
            ++not_match_cnt;
            serverAssert(0);
            // debug_print_not_match_lru(evict_hash, field_in_hash);
        } else {
            sds field_in_lru = dictGetKey(check_in_lru);
            serverAssert(field_in_hash == field_in_lru);
        }
    }
    dictReleaseIterator(di);
    // if (not_match_cnt == 0) serverLog(LL_WARNING, "debug_check_field_same() successfully!");
    return not_match_cnt;
}

static void debug_cron_check() {
    for (int i = 0; i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
        evictHash *evict_hash = server.evic_hash_candidates + i;
        if (!evict_hash->used) continue;

        debug_check_evict_hash(evict_hash);
    }
}
*/

static void debugReportMemAndKey() {
    size_t used = zmalloc_used_memory();
    size_t mem_tofree = 0;
    if (used > server.bunnymem)
        mem_tofree = used - server.bunnymem;

    serverLog(LL_WARNING, "to free = %zu, used = %zu, bunnymem = %llu", mem_tofree, used, server.bunnymem);

    dictIterator *di;
    dictEntry *de;
    for (int i = 0; i < server.dbnum; ++i) {
        int key_cnt = 0;
        int str_cnt = 0;
        int str_raw_emb_cnt = 0;
        int str_rock_cnt = 0;
        int hash_cnt = 0;
        int pure_hash_cnt = 0;
        int ziplist_cnt = 0;
        int ziplist_rock_cnt = 0;

        di = dictGetIterator(server.db[i].dict);
        while ((de = dictNext(di))) {
            ++key_cnt;
            robj *o = dictGetVal(de);
            if (o->type == OBJ_STRING)
            {
                ++str_cnt;
                if (o->encoding == OBJ_ENCODING_RAW || o->encoding == OBJ_ENCODING_EMBSTR) {
                    str_raw_emb_cnt++;
                } 
                if (o == shared.keyRockVal)
                    ++str_rock_cnt;
            } else if (o->type == OBJ_HASH) {
                ++hash_cnt;
                if (o->encoding == OBJ_ENCODING_HT) {
                    ++pure_hash_cnt;
                } else {
                    ++ziplist_cnt;
                }
                if (o == shared.ziplistRockVal)
                    ++ziplist_rock_cnt;
            }       
        }
        dictReleaseIterator(di);

        if (key_cnt) {
            redisDb *db = server.db+i;
            serverLog(LL_WARNING, 
                      "dbid = %d, key_cnt = %d, hash_cnt = %d, pure_hash_cnt = %d, no_rock_total = %lu", 
                      i, key_cnt, hash_cnt, pure_hash_cnt, dictSize(db->str_zl_norock_keys));
            serverLog(LL_WARNING, "       str cnt(really) = %d, str of raw or embed(really) = %d, stat of str = %lld, ", 
                      str_cnt, str_raw_emb_cnt, db->stat_key_str_cnt);
            serverLog(LL_WARNING, "       str rock cnt(really) = %d, stat of str rock = %lld", 
                      str_rock_cnt, db->stat_key_str_rockval_cnt);
            serverLog(LL_WARNING, "       ziplist cnt(really) = %d, stat of ziplist = %lld", 
                      ziplist_cnt, db->stat_key_ziplist_cnt);
            serverLog(LL_WARNING, "       ziplist rock cnt(really) = %d, stat of ziplist rock cnt = %lld", 
                      ziplist_rock_cnt, db->stat_key_ziplist_rockval_cnt);
        }
    }
}

static void debugReportMemAndHash() {
    size_t used = zmalloc_used_memory();
    size_t mem_tofree = 0;
    if (used > server.bunnymem)
        mem_tofree = used - server.bunnymem;

    serverLog(LL_WARNING, "to free = %zu, used = %zu, bunnymem = %llu", mem_tofree, used, server.bunnymem);

    for (int i = 0; i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
        evictHash *evict_hash = server.evic_hash_candidates + i;
        if (!evict_hash->used) continue;

        serverLog(LL_WARNING, "hash candidates, dbid = %u, key = %s, field cnt = %lu, lru cnt = %lu, rock cnt = %lld, no_rockss size = %lu",
                  evict_hash->dbid, evict_hash->key, 
                  dictSize(evict_hash->dict_hash), dictSize(evict_hash->field_lru),
                  evict_hash->rock_cnt, dictSize(evict_hash->no_rocks));
    }
}

static void debugReportMem() {
    debugReportMemAndKey();
    debugReportMemAndHash();
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm. 
 * Refercnet evict.c estimateObjectIdleTime() for more details */
unsigned long long estimateObjectIdleTimeFromLruDictEntry(dictEntry *de) {
    unsigned long long lruclock = LRU_CLOCK();
    unsigned long long my_lru = de->v.u64;
    if (lruclock >= my_lru) {
        return (lruclock - my_lru) * LRU_CLOCK_RESOLUTION;
    } else {
        return (lruclock + (LRU_CLOCK_MAX - my_lru)) * LRU_CLOCK_RESOLUTION;
    }
}

static int getSomeKeysForStringAndZiplist(dict *dict_sample, dict *dict_real,
                                          dictEntry **de_samples, 
                                          dict* dict_lru, dictEntry **de_lrus) {
    dictEntry *original_samples[server.maxmemory_samples];                                  
    int count = dictGetSomeKeys(dict_sample, original_samples, server.maxmemory_samples);

    int valid_count = 0;
    for (int i = 0; i < count; ++i) {
        sds key = dictGetKey(original_samples[i]);
        // robj *o = dictGetVal(original_samples[i]);
        dictEntry *de = dictFind(dict_real, key);
        serverAssert(de);
        robj *o = dictGetVal(de);

        int is_string = (o->type == OBJ_STRING && (o->encoding == OBJ_ENCODING_RAW || o->encoding == OBJ_ENCODING_EMBSTR));
        int is_ziplist = (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_ZIPLIST);

        // We change the sample dict, so there is a guarantee
        serverAssert((is_string || is_ziplist) && o != shared.keyRockVal);

        if (o->refcount != OBJ_SHARED_REFCOUNT) {
            de_samples[valid_count] = de;
            dictEntry *de_lru = dictFind(dict_lru, key);
            serverAssert(de_lru);
            de_lrus[valid_count] = de_lru;
            ++valid_count;
        }
    }
    return valid_count;
}

static int getSomeKeysForPureHash(dict *dict_sample, dict *dict_real, dictEntry **de_samples, 
                                  dict* dict_lru, dictEntry **de_lrus) {
    dictEntry *original_samples[server.maxmemory_samples];                                  
    int count = dictGetSomeKeys(dict_sample, original_samples, server.maxmemory_samples);

    for (int i = 0; i < count; ++i) {
        sds field = dictGetKey(original_samples[i]);
        dictEntry *de = dictFind(dict_real, field);
        serverAssert(de);
        serverAssert(dictGetVal(de) != shared.hashRockVal);

        dictEntry *de_lru = dictFind(dict_lru, field);
        serverAssert(de_lru);

        de_samples[i] = de;
        de_lrus[i] = de_lru;
    }
    return count;
}

/* NOTE: does not like evict.c similiar function, we do not evict anything from TTL dict */
static size_t evictKeyPoolPopulate(int dbid, dict *sample_dict, dict *real_dict, dict *lru_dict, struct evictKeyPoolEntry *pool) {

    dictEntry *samples[server.maxmemory_samples];
    dictEntry *lru_samples[server.maxmemory_samples];
    int count = getSomeKeysForStringAndZiplist(sample_dict, real_dict, samples, lru_dict, lru_samples);

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
static size_t evictPureHashPoolPopulate(int dbid, sds key, 
                                        dict *sample_dict, 
                                        dict *real_dict, dict *lru_dict, struct evictHashPoolEntry *pool) {

    dictEntry *samples[server.maxmemory_samples];
    dictEntry *lru_samples[server.maxmemory_samples];
    int count = getSomeKeysForPureHash(sample_dict, real_dict, samples, lru_dict, lru_samples);

    size_t insert_cnt = 0;

    int j, k;
    unsigned long long idle;
    sds field;
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

/* if return is bigger than 100, it means total cnt is zero */
/*
static int getRockKeyOfStringAndZiplistPercentage() {
    long long str_cnt = 0;
    long long str_rock_cnt = 0;
    long long ziplist_cnt = 0;
    long long ziplist_rock_cnt = 0;
    redisDb *db;

    for (int i = 0; i < server.dbnum; ++i) {
        db = server.db+i;
        str_cnt += db->stat_key_str_cnt;
        str_rock_cnt += db->stat_key_str_rockval_cnt;
        ziplist_cnt += db->stat_key_ziplist_cnt;
        ziplist_rock_cnt += db->stat_key_ziplist_rockval_cnt;
    }
    long long total_cnt = str_cnt + ziplist_cnt;

    return total_cnt == 0 ? 101 :(int)((long long)100 * (str_rock_cnt + ziplist_rock_cnt) / total_cnt);    
}
*/

static size_t can_evict_key_zl_size(size_t *str_zl_cnt) {
    size_t no_rock_total = 0;
    if (str_zl_cnt) *str_zl_cnt = 0;

    for (int i = 0; i < server.dbnum; ++i) {
        redisDb *db = server.db + i;
        no_rock_total += dictSize(db->str_zl_norock_keys);
        if (str_zl_cnt) {
            serverAssert(db->stat_key_str_cnt >= 0);
            *str_zl_cnt = *str_zl_cnt + db->stat_key_str_cnt;
            serverAssert(db->stat_key_ziplist_cnt >= 0);
            *str_zl_cnt = *str_zl_cnt + db->stat_key_ziplist_cnt;
        }
    }

    return no_rock_total;
} 

/* return EVICT_ROCK_FREE    if evict enough memory 
 * return EVICT_ROCK_TIMEOUT if timeout and not enought memory has been released 
 * If must_od is ture, we ignore the memory over limit check */
static int performKeyOfStringOrZiplistEvictions(int must_do, size_t must_tofree, size_t *real_free) {
    int keys_freed = 0;
    size_t mem_tofree;
    long long mem_freed; /* May be negative */
    long long delta;
    
    if (must_do) {
        mem_tofree = must_tofree;
    } else {
        size_t used = zmalloc_used_memory();
        if(used <= server.bunnymem) 
            return EVICT_ROCK_FREE;

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
        // robj *valstrobj;
        robj *valobj;       // string or hash with ziplisst
        int bestdbid;
        redisDb *db;
        dict *lru_dict;
        dict *dict;
        dictEntry *de;

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
                    // size_t insert_cnt = evictKeyPoolPopulate(i, dict, lru_dict, pool);
                    // We change sample dict
                    size_t insert_cnt = evictKeyPoolPopulate(i, db->str_zl_norock_keys, dict, lru_dict, pool);
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
                    valobj = (robj*)dictGetVal(de);
                    int type_correct = (valobj->type == OBJ_STRING 
                                        && (valobj->encoding == OBJ_ENCODING_RAW || valobj->encoding == OBJ_ENCODING_EMBSTR)) ||
                                       (valobj->type == OBJ_HASH && valobj->encoding == OBJ_ENCODING_ZIPLIST);
                    if (type_correct && valobj->refcount != OBJ_SHARED_REFCOUNT)
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
            if (valobj->type == OBJ_STRING) {
                serverAssert((valobj->encoding == OBJ_ENCODING_RAW || valobj->encoding == OBJ_ENCODING_EMBSTR) && 
                             valobj->refcount != OBJ_SHARED_REFCOUNT);
                dictSetVal(dict, de, shared.keyRockVal);
                ++db->stat_key_str_rockval_cnt;
                addRockWriteTaskOfString(bestdbid, bestkey, valobj->ptr);                
            } else {
                serverAssert(valobj->type == OBJ_HASH && valobj->encoding == OBJ_ENCODING_ZIPLIST && 
                             valobj->refcount != OBJ_SHARED_REFCOUNT);
                dictSetVal(dict, de, shared.ziplistRockVal);
                ++db->stat_key_ziplist_rockval_cnt;
                unsigned char *zl = valobj->ptr;
                addRockWriteTaskOfZiplist(bestdbid, bestkey, zl);                
            }
            decrRefCount(valobj);
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

    if (real_free) *real_free = mem_freed > 0 ? mem_freed : 0;
    return timeout ? EVICT_ROCK_TIMEOUT : EVICT_ROCK_FREE;
}

/* if return is bigger than 100, it means fieldCnt is zero */
/*
static int getRockKeyOfPureHashPercentage() {
    long long field_cnt = 0;
    long long rock_cnt = 0;

    for (int i = 0; i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
        evictHash *evict_hash = server.evic_hash_candidates + i;
        if (!evict_hash->used) continue;

        field_cnt += dictSize(evict_hash->dict_hash);
        rock_cnt = evict_hash->rock_cnt;
    }

    return field_cnt == 0 ? 101 :(int)((long long)100 * rock_cnt / field_cnt);    
}
*/

static size_t can_evict_pure_hash_size(size_t *field_cnt) {
    size_t no_rock_total = 0;
    if (field_cnt) *field_cnt = 0;

    for (int i = 0; i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
        evictHash *evict_hash = server.evic_hash_candidates + i;
        if (!evict_hash->used) continue;

        no_rock_total += dictSize(evict_hash->no_rocks);
        if (field_cnt) *field_cnt = *field_cnt + dictSize(evict_hash->dict_hash);
    }

    return no_rock_total;
} 

/* return EVICT_ROCK_FREE    if evict enough memory 
 * return EVICT_ROCK_TIMEOUT if timeout and not enought memory has been released 
 * If must_od is ture, we ignore the memory over limit check */
static int performKeyOfPureHashEvictions(int must_do, size_t must_tofree, size_t *real_free) {
    // return EVICT_ROCK_ENOUGH_MEM;   // debug

    int keys_freed = 0;
    size_t mem_tofree;
    long long mem_freed; /* May be negative */
    long long delta;

    size_t used = zmalloc_used_memory();
    if (must_do) {
        if (used >= server.bunnymem) {
            mem_tofree = used - server.bunnymem > must_tofree ? used - server.bunnymem : must_tofree;
        } else {
            mem_tofree = must_tofree;
        }
    } else {
        if(used <= server.bunnymem) 
            return EVICT_ROCK_FREE;

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
        dictEntry *de_of_db;
        dict *dict_of_hash;
        dictEntry *de_of_hash;

        int fail_cnt = 0;
        // unsigned long total_keys, keys;
        unsigned long total_keys;
        int can_not_free = 0;
        while(bestfield == NULL && fail_cnt < 100) {
            total_keys = 0;

            /* We don't want to make local-db choices when expiring fields,
             * so to start populate the eviction pool sampling keys from
             * every evict_hash_candidates for all dbs. */
            for (int i = 0; i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
                evictHash *evict_hash = server.evic_hash_candidates + i;
                if (!evict_hash->used) continue;
                total_keys += evictPureHashPoolPopulate(evict_hash->dbid, evict_hash->key,
                                                        evict_hash->no_rocks,
                                                        evict_hash->dict_hash, evict_hash->field_lru, pool);
            }

            /* No insert keys and pool is empty, skip to evict. */
            if (total_keys == 0 && pool[0].field == NULL) {
                can_not_free = 1;
                break;
            } 

            /* Go backward from best to worst element to evict. */
            for (k = EVPOOL_SIZE-1; k >= 0; k--) {
                if (pool[k].field == NULL) {
                    bestfield = NULL;
                    continue;
                }

                bestdbid = pool[k].dbid;
                // bestfield = pool[k].field;
                // bestkeyWithField = pool[k].key;

                de_of_hash = NULL;
                de_of_db = dictFind(server.db[bestdbid].dict, pool[k].key);
                if (de_of_db) {
                    bestkeyWithField = dictGetKey(de_of_db);
                    robj *o = dictGetVal(de_of_db);
                    if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT && o->refcount != OBJ_SHARED_REFCOUNT) {
                        dict_of_hash = o->ptr;
                        de_of_hash = dictFind(dict_of_hash, pool[k].field);
                    }
                }

                /* Remove the entry from the pool. */
                if (pool[k].field != pool[k].cached_field)
                    sdsfree(pool[k].field);
                if (pool[k].key != pool[k].cached_key)
                    sdsfree(pool[k].key);
                pool[k].field = NULL;
                pool[k].key = NULL;
                pool[k].idle = 0;
                pool[k].dbid = -1;

                /* If the key exists, is our pick. Otherwise it is
                 * a ghost and we need to try the next element. */
                /* What is the situation for ghost? */
                /* The pool's element has previous value which could be changed, */
                /*     e.g. DEL and change anotther type */
                /* These kinds of elements in the pool  which are not string type */
                /*     or value has been saved to Rocksdb are Ghost */
                int ghost = 1;
                if (de_of_hash) {
                    valstrsds = dictGetVal(de_of_hash);
                    if (valstrsds != shared.hashRockVal) 
                        ghost = 0;          // not a ghost
                } 
                if (!ghost) {
                    bestfield = dictGetKey(de_of_hash);
                    break;
                }
            }
            if (bestfield == NULL) ++fail_cnt;
        }

        if (can_not_free) {
            timeout = 1;
            break;
        }

        /* Finally convert value of the selected key and write it to RocksDB write queue */
        if (bestfield) {
            serverAssert(bestkeyWithField);
            serverAssert(bestdbid >= 0 && bestdbid < server.dbnum);
            delta = (long long) zmalloc_used_memory();
            // dictSetVal(dict_of_hash, de_of_hash, shared.hashRockVal);
            dictGetVal(de_of_hash) = shared.hashRockVal;
            // update stat if possible
            evictHash *evict_hash = lookupEvictOfHash(bestdbid, bestkeyWithField);
            if (evict_hash) {
                ++evict_hash->rock_cnt;
                int ret = dictDelete(evict_hash->no_rocks, bestfield);
                serverAssert(ret == DICT_OK);
            }
            addRockWriteTaskOfHash(bestdbid, bestkeyWithField, bestfield, valstrsds);
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

    if (real_free) *real_free = mem_freed > 0 ? mem_freed : 0;
    return timeout ? EVICT_ROCK_TIMEOUT : EVICT_ROCK_FREE;
}

/* We have two choice of eviction, key (include hash with ziplist) or pure hash. We try randomly to use one. 
 * We always choose the higher percentage one 
 * RETURN is EVICT_ROCK_ENOUGH_MEM, EVICT_ROCK_NOT_READY, EVICT_ROCK_FREE or EVICT_ROCK_TIMEOUT */
// #define CONTINUOUS_EVICT_MAX 8
int performeKeyOrHashEvictions(int must_do, size_t must_tofree, size_t *real_free) {
    if (!must_do && zmalloc_used_memory() <= server.bunnymem)
        // used memory not over limit and not must_do
        return EVICT_ROCK_ENOUGH_MEM;        

    size_t str_zl_cnt;
    size_t str_zl_no_rock_total = can_evict_key_zl_size(&str_zl_cnt);
    serverAssert(str_zl_cnt >= str_zl_no_rock_total);
    size_t pure_field_cnt;
    size_t pure_field_no_rock_total = can_evict_pure_hash_size(&pure_field_cnt);
    serverAssert(pure_field_cnt >= pure_field_no_rock_total);

    if (str_zl_no_rock_total == 0 && pure_field_no_rock_total == 0) 
        return EVICT_ROCK_NOT_READY;

    int do_str_zl;
    
    if (pure_field_no_rock_total == 0) {
        do_str_zl = 1;
    } else if (str_zl_no_rock_total == 0) {
        do_str_zl = 0;
    } else {
        size_t perc_str_zl = str_zl_no_rock_total*1000/str_zl_cnt;
        size_t perc_pur_hash =  pure_field_no_rock_total*1000/pure_field_cnt;
    
        if (perc_str_zl == perc_pur_hash) {
            if (str_zl_no_rock_total >= pure_field_no_rock_total) {
                do_str_zl = 1;
            } else {
                do_str_zl = 0;
            }
        } else if (perc_str_zl > perc_pur_hash) {
            do_str_zl = 1;
        } else {
            do_str_zl = 0;
        }
    }

    int ret;
    if (do_str_zl) {
        ret = performKeyOfStringOrZiplistEvictions(must_do, must_tofree, real_free);
    } else {
        ret = performKeyOfPureHashEvictions(must_do, must_tofree, real_free);
    }
    serverAssert(ret != EVICT_ROCK_NOT_READY);
    return ret;

/*
    static int dice = 0;
    static int evict_str_cnt = 0;
    static int evict_hash_cnt = 0;

    if (dice == 0 && evict_str_cnt > 0) {
        if (evict_str_cnt >= CONTINUOUS_EVICT_MAX) {
            dice = 1;   // over continuous limit, must change dice
            evict_str_cnt = 0;
        } 
    } else if (dice == 1 && evict_hash_cnt > 0) {
        if (evict_hash_cnt >= CONTINUOUS_EVICT_MAX) {
            dice = 0;   // over continuous limit, must change dice
            evict_hash_cnt = 0;
        } 
    } else {
        evict_str_cnt = 0;
        evict_hash_cnt = 0;
    }

    if (dice == 0) {
        int try_str_res = performKeyOfStringOrZiplistEvictions(must_do, must_tofree, real_free);
        if (try_str_res == EVICT_ROCK_ENOUGH_MEM) {
            return EVICT_ROCK_ENOUGH_MEM;       
        } else if (try_str_res == EVICT_ROCK_FREE) {
            ++evict_str_cnt;
            return EVICT_ROCK_FREE;     
        } else {
            // we must try another way
            dice = 1;
            int try_hash_res = performKeyOfPureHashEvictions(must_do, must_tofree, real_free);
            if (try_hash_res == EVICT_ROCK_ENOUGH_MEM) {
                return EVICT_ROCK_ENOUGH_MEM;
            } else if (try_hash_res == EVICT_ROCK_FREE) {
                ++evict_hash_cnt;
                return EVICT_ROCK_FREE;
            } else {
                dice = 0;
                return try_hash_res;
            }
        }        
    } else {
        serverAssert(dice == 1);
        int try_hash_res = performKeyOfPureHashEvictions(must_do, must_tofree, real_free);
        if (try_hash_res == EVICT_ROCK_ENOUGH_MEM) {
            return EVICT_ROCK_ENOUGH_MEM;
        } else if (try_hash_res == EVICT_ROCK_FREE) {
            ++evict_hash_cnt;
            return EVICT_ROCK_FREE;
        } else {
            // we must try another way
            dice = 0;
            int try_str_res = performKeyOfStringOrZiplistEvictions(must_do, must_tofree, real_free);
            if (try_str_res == EVICT_ROCK_ENOUGH_MEM) {
                return EVICT_ROCK_ENOUGH_MEM;
            } else if (try_str_res == EVICT_ROCK_FREE) {
                ++evict_str_cnt;
                return EVICT_ROCK_FREE;
            } else {
                dice = 1;
                return try_str_res;
            }
        }
    }
*/
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
// #define ENOUGH_MEM_SPACE 50<<20         // if we have enought free memory of 50M, do not need to evict
#define FREE_LOWER_BOUND 1<<20
#define FREE_UPPER_BOUND 20<<20
void cronEvictToMakeRoom() {
    size_t used = zmalloc_used_memory();
    if (used <= server.bunnymem) 
        return;    

    size_t delta = used - server.bunnymem;
    size_t must_free;
    if (delta >= FREE_UPPER_BOUND) {
        must_free = FREE_UPPER_BOUND;
    } else if (delta < FREE_LOWER_BOUND) {
        must_free = FREE_LOWER_BOUND;
    } else {
        must_free = delta;
    }

    static size_t total_free_in_cron = 0;
    size_t real_free = 0;
    int res = performeKeyOrHashEvictions(1, must_free, &real_free);     // must free 1 Megabyte

    if (res == EVICT_ROCK_NOT_READY) {
        if (is_startup_on_going()) {
            debugReportMemAndKey();
            serverLog(LL_WARNING, "!!!!!!!!No keys to evict in startup, increase bunnymem or real memory!!!!!!!!!!");
            exit(1);
        } else {
            total_free_in_cron = 0;
            return;
        }
    }
    total_free_in_cron += real_free;

    static int over_pencentage_cnt = 0;
    ++over_pencentage_cnt;
    over_pencentage_cnt = over_pencentage_cnt % 128;
    if (res == EVICT_ROCK_ENOUGH_MEM || res == EVICT_ROCK_FREE) {
        total_free_in_cron = 0;
        over_pencentage_cnt = 0;
    } else {
        serverAssert(res == EVICT_ROCK_TIMEOUT);
        if (over_pencentage_cnt == 0) {
            // if Hz 50, so 2 seconds report once
            serverLog(LL_WARNING, "memory is over limit. I have evicted %lu(k), but need do more in future...",
                      total_free_in_cron/1024);
            total_free_in_cron = 0;
        }
    }

    /*
    if (res == EVICT_ROCK_NOT_READY) {
        ++over_pencentage_cnt;
        over_pencentage_cnt = over_pencentage_cnt % 128;        // if Hz 50, so 2 seconds report once
        if (over_pencentage_cnt == 0) {
            serverLog(LL_WARNING, "memory is over limit. I have evicted %lu(k), but need do more in future...",
                      total_free_in_cron/1000);
            total_free_in_cron = 0;
        }
    } else if (res == EVICT_ROCK_NOT_READY) {
        serverLog(LL_WARNING, "memory is over limit and high percentage of dataset in Rock already. I can not do more in future!");
    } else if (res == EVICT_ROCK_NOT_READY || res == EVICT_ROCK_TIMEOUT) {
        total_free_in_cron += real_free;
    }
    */
}

/* find evictHash in server.evict_hash_candidates 
 * If not found return NULL. We use cached for memory because hash evict call it frequently */
evictHash* lookupEvictOfHash(const uint8_t dbid, sds key) {
    for (int i = 0; i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
        evictHash *evict_hash = server.evic_hash_candidates + i;
        if (!evict_hash->used) continue;

        if (evict_hash->dbid == dbid && (evict_hash->key == key || sdscmp(evict_hash->key, key) == 0))
            return evict_hash;
    }   
    return NULL;
}

static evictHash* findSmallestHashCandidate() {
    // We only check when server.evict_hash_candidates is full
    for (int i = 0; i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
        evictHash *evict_hash = server.evic_hash_candidates + i;
        serverAssert(evict_hash->used);
    }

    evictHash *smallest = server.evic_hash_candidates;
    for (int i = 1; i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
        evictHash *evict_hash = server.evic_hash_candidates + i;
        if (dictSize(evict_hash->dict_hash) < dictSize(smallest->dict_hash))
            smallest = evict_hash;
    }
    return smallest;
}

/* if exists return 1. otherwise return 0 */
static int exist_in_candidates(uint8_t dbid, sds key) {
    for (int i = 0; i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
        evictHash *evict_hash = server.evic_hash_candidates + i;
        if (!evict_hash->used) continue;

        if (evict_hash->dbid == dbid && (evict_hash->key == key || sdscmp(evict_hash->key, key) == 0))
            return 1;
    }
    return 0;
}

static evictHash* find_free_candidatee() {
    for (int i = 0; i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
        evictHash *evict_hash = server.evic_hash_candidates + i;
        if (!evict_hash->used) return evict_hash;
    }
    return NULL;
}

/* If Success remove return the removed. otherwise return NULL
 * reference constructEvictHash() to see how release memory 
 * NOTE: 
 *      if input parameter combined_key is the same of the internal, does not free. The caller need to do this */
evictHash* removeHashCandidate(uint8_t dbid, sds key) {
    for (int i = 0;  i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
        evictHash *evict_hash = server.evic_hash_candidates + i;
        if (!evict_hash->used) continue;

        if (evict_hash->dbid == dbid && (evict_hash->key == key || sdscmp(evict_hash->key, key) == 0)) {
            // clear memroy for the evict_hash
            // sdsfree(evict_hash->combined_key);
            sdsfree(evict_hash->key);
            dictRelease(evict_hash->field_lru);
            dictRelease(evict_hash->no_rocks);
            memset(evict_hash, 0, sizeof(*evict_hash));
            return evict_hash;
        }
    }
    return NULL;
}

/* remove all candidates for one dbid 
 * If remove any, return 1, otheerwise return 0 */
static int removeCandidatesByDbid(uint8_t dbid) {
    int removed_any = 0;
    for (int i = 0;  i < EVICT_HASH_CANDIDATES_MAX_SIZE; ++i) {
        evictHash *evict_hash = server.evic_hash_candidates + i;
        if (!evict_hash->used) continue;

        if (evict_hash->dbid == dbid) {
            evictHash *removed = removeHashCandidate(evict_hash->dbid, evict_hash->key);
            serverAssert(removed);
            removed_any = 1;
        }
    }
    return removed_any;
}

static void constructEvictHash(evictHash *free_candidate, uint8_t dbid, sds key) {
    serverAssert(!free_candidate->used);
    redisDb *db = server.db + dbid;
    dictEntry *de_db = dictFind(db->dict, key);
    serverAssert(de_db);
    robj *o = dictGetVal(de_db);
    serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT && o->refcount != OBJ_SHARED_REFCOUNT);
    free_candidate->dict_hash = o->ptr;

    // sds combined_key = sdsnewlen(&dbid, 1);
    // combined_key = sdscatlen(combined_key, key, sdslen(key));
    serverAssert(!free_candidate->field_lru);
    free_candidate->field_lru = dictCreate(&fieldLruDictType, NULL);
    serverAssert(!free_candidate->no_rocks);
    free_candidate->no_rocks = dictCreate(&pureHashNoRockDictType, NULL);

    long long rock_cnt = 0;
    dictIterator *di = dictGetIterator(free_candidate->dict_hash);
    dictEntry *de;
    while ((de = dictNext(di))) {
        sds field = dictGetKey(de);
        uint64_t clock = LRU_CLOCK();
        dictAdd(free_candidate->field_lru, field, (void*)clock);    // field_lru use the same field with dict_hash 
        sds val = dictGetVal(de);
        if (val == shared.hashRockVal) {
            ++rock_cnt;
        } else {
            int ret = dictAdd(free_candidate->no_rocks, field, 0);        // no_rocks use the same field with dict_hash
            serverAssert(ret == DICT_OK);
        }
    }
    dictReleaseIterator(di);
    free_candidate->key = sdsdup(key);
    free_candidate->rock_cnt = rock_cnt;
    serverAssert(rock_cnt == (long long)dictSize(free_candidate->dict_hash) - (long long)dictSize(free_candidate->no_rocks));
    free_candidate->used = 1;
}

/* caller guarantee the dbid+key object is hash type with hash encoding and not shared
 * If success added, return 1, otherwise return 0 */
static int addToHashCandidatesIfPossible(const uint8_t dbid, const sds key) {
    // if already in the candiates, no need to add
    if (exist_in_candidates(dbid, key)) return 0;

    evictHash *free_candidate = find_free_candidatee();
    if (free_candidate) {
        constructEvictHash(free_candidate, dbid, key);
        return 1;
    } else {
        evictHash *smallest = findSmallestHashCandidate();
        serverAssert(smallest);
        redisDb *db = server.db + dbid;
        dictEntry *de_db = dictFind(db->dict, key);
        robj *o = dictGetVal(de_db);
        serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT && o->refcount != OBJ_SHARED_REFCOUNT);
        dict *dict_hash = o->ptr;
        if (dictSize(smallest->dict_hash) < dictSize(dict_hash)) {
            evictHash *removed = removeHashCandidate(smallest->dbid, smallest->key);
            serverAssert(removed);
            constructEvictHash(removed, dbid, key);
            return 1;
        } else {
            return 0;
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
int checkAddToEvictHashCandidates(const uint8_t dbid, const size_t added, sds key) {
    if (added == 0) return 0;
    
    redisDb *db = server.db+dbid;
    dictEntry *de_db = dictFind(db->dict, key);
    serverAssert(de_db);
    robj *o = dictGetVal(de_db);
    serverAssert(o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT && o->refcount != OBJ_SHARED_REFCOUNT);
    dict *dict_hash = o->ptr;

    size_t now = dictSize(dict_hash);
    serverAssert(now >= added);
    size_t before = now - added;

    if (now/CHECK_INTERVAL != before/CHECK_INTERVAL) {
        return addToHashCandidatesIfPossible(dbid, key);
    } else {
        return 0;   // now added for over CHECK_INTERVAL, no check
    }    
}

/* because flush db use simple remove dict code, we need to clear all resource relating to rock */
void clearEvictByEmptyDb(uint8_t dbid) {
    serverAssert(dbid < server.dbnum);
    removeCandidatesByDbid(dbid);
}

/* the object is the obj in db->dict */
void updatePureHashLru(uint8_t dbid, sds key, sds field) {
    evictHash *evict_hash = lookupEvictOfHash(dbid, key);
    if (evict_hash) {
        dict *lru = evict_hash->field_lru;
        dictEntry *de = dictFind(lru, field);
        if (de) {
            uint64_t clock = LRU_CLOCK();
            dictGetVal(de) = (void*)clock;
        }
    }
}

/* update all lru for the key in the dbid if in candiates */
void updatePureHashLruForWholeKey(uint8_t dbid, sds key) {
    evictHash *evict_hash = lookupEvictOfHash(dbid, key);
    if (evict_hash) {
        uint64_t clock = LRU_CLOCK();
        dict* lru = evict_hash->field_lru;
        dictIterator *di = dictGetIterator(lru);
        dictEntry *de;
        while ((de = dictNext(di))) {
            dictGetVal(de) = (void*)clock;
        }
        dictReleaseIterator(di);
    }
}

void updateKeyLruForOneKey(uint8_t dbid, sds key) {
    redisDb *db = server.db + dbid;
    dictEntry *de = dictFind(db->key_lrus, key);
    serverAssert(de);
    uint64_t clock = LRU_CLOCK();
    dictGetVal(de) = (void*)clock;
}

void debugEvictCommand(client *c) {
    sds flag = c->argv[1]->ptr;

    if (strcasecmp(flag, "evictkey") == 0) {
        serverLog(LL_WARNING, "debugEvictCommand() for key has been called!");
        serverLog(LL_WARNING, "===== before evcition ===========");
        debugReportMemAndKey();
        serverLog(LL_WARNING, "===== after evcition ===========");
        int res = performKeyOfStringOrZiplistEvictions(0, 0, NULL);
        char *str_res;
        switch(res) {
        case EVICT_ROCK_ENOUGH_MEM: str_res = "EVICT_ROCK_ENOUGH_MEM"; break;
        case EVICT_ROCK_NOT_READY: str_res = "EVICT_ROCK_NOT_READY"; break;
        case EVICT_ROCK_FREE: str_res = "EVICT_ROCK_FREE"; break;
        case EVICT_ROCK_TIMEOUT: str_res = "EVICT_ROCK_TIMEOUT"; break;
        default: serverPanic("debugEvictCommand(), No such case!");
        }
        serverLog(LL_WARNING, "performKeyOfStringEvictions() res = %s", str_res);
        debugReportMemAndKey();
    } else if (strcasecmp(flag, "evicthash") == 0) {
        serverLog(LL_WARNING, "debugEvictCommand() for hash has been called!");
        serverLog(LL_WARNING, "===== before evcition ===========");
        debugReportMemAndHash();
        serverLog(LL_WARNING, "===== after evcition ===========");
        int res = performKeyOfPureHashEvictions(0, 0, NULL);
        char *str_res;
        switch(res) {
        case EVICT_ROCK_ENOUGH_MEM: str_res = "EVICT_ROCK_ENOUGH_MEM"; break;
        case EVICT_ROCK_NOT_READY: str_res = "EVICT_ROCK_NOT_READY"; break;
        case EVICT_ROCK_FREE: str_res = "EVICT_ROCK_FREE"; break;
        case EVICT_ROCK_TIMEOUT: str_res = "EVICT_ROCK_TIMEOUT"; break;
        default: serverPanic("debugEvictCommand(), No such case!");
        }
        serverLog(LL_WARNING, "performKeyOfHashEvictions() res = %s", str_res);
        debugReportMemAndHash();
    } else if (strcasecmp(flag, "reportkey") == 0) {
        debugReportMemAndKey();
    } else if (strcasecmp(flag, "reporthash") == 0) {
        debugReportMemAndHash();
    } else if (strcasecmp(flag, "report") == 0) {
        debugReportMem();
    } else {
        addReplyError(c, "wrong flag for debugevict!");
        return;
    }

    addReplyBulk(c,c->argv[0]);
}
