
/* BunnyRedis is based on Redis, coded by Tony. The copyright is same as Redis.
 *
 * Copyright (c) 2018, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "rock.h"
#include "streamwrite.h"
#include "rockevict.h"

#include "assert.h"
#include <rocksdb/c.h>
#include <ftw.h>


#define ROCKSDB_LEVEL_NUM   7

#define START_SLEEP_MICRO   16
#define MAX_SLEEP_MICRO     1024            // max sleep for 1 ms

#define ROCK_STRING_TYPE    0       // For Redis String(encoding is str too). Rocksdb key is coded as dbid(uint8_t) + byte key of Redis string type
#define ROCK_HASH_TYPE      1       // For Redis Hash(encoding is hash too). Rocksdb key is coded as dbid(uint8_t) + (uint32_t) key_size + byte of key + byte of field
#define ROCK_ZIPLIST_TYPE   2       // For Redis Hash(encoding is Ziplist). Rocksdb key is coded as dbid(uint8_t) + byte key of Reds string type 

#define ROCK_WRITE_QUEUE_TOO_LONG   256
#define ROCK_WRITE_PICK_MAX_LEN     1024

// Below is spin lock support
// spin lock only run in Linux
static pthread_spinlock_t readLock;     
static pthread_spinlock_t writeLock;

static void lockRockRead() {
    int res = pthread_spin_lock(&readLock);
    serverAssert(res == 0);
}

static void unlockRockRead() {
    int res = pthread_spin_unlock(&readLock);
    serverAssert(res == 0);
}

static void lockRockWrite() {
    int res = pthread_spin_lock(&writeLock);
    serverAssert(res == 0);
}

static void unlockRockWrite() {
    int res = pthread_spin_unlock(&writeLock);
    serverAssert(res == 0);
}

// below is stream data structure
rocksdb_t *rockdb = NULL;

struct WriteTask {
    // uint8_t type;
    sds rock_key;
    sds val;
};

list* write_queue;      // list of *WriteTask

void valAsListDestructor(void *privdata, void *obj) {
    UNUSED(privdata);

    listRelease(obj);
}

void keyInRockDestructor(void *privdata, void *obj) {
    UNUSED(privdata);

    sdsfree(obj);
}

/* rock key hashtable. The value is a list of client id */
dictType readCandidatesDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    keyInRockDestructor,        /* key destructor, we stored the clientid in the key pointer */
    valAsListDestructor,        /* val destructor */
    NULL                        /* allow to expand */
};

/* key is sds in readCandidatesDictType. value is de* in readCandidatesDictType */
dictType streamWaitKeyDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor, we stored the clientid in the key pointer */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

// stream_wait_keys is a set. Set key is sds key which stream write is waiting for. 
// And the sds key share the same object in read_candidates. So does not need to be freed
dict *stream_wait_rock_keys;     
// read_candidates is a dictionary. Dict key is sds key as reading task. 
// Dict val is a list of client id waiting for the key. 
// NOTE: the list could have duplicated client id. 
//       e.g., multi -> exec could leed to duplicated rock key with the same client.
dict *read_rock_key_candidates;

#define MAX_READ_TASK_NUM   16
size_t on_fly_task_cnt;        // how may task for the read thraed to do
sds on_fly_rock_keys[MAX_READ_TASK_NUM];
sds on_fly_vals[MAX_READ_TASK_NUM];        // if first element is NULL, it means no return values
sds val_not_found;

// reference https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
static void report_rocksdb_mem_usage() {
    if (!rockdb) return;

    char* info;

    info = rocksdb_property_value(rockdb, "rocksdb.block-cache-usage");
    if (info) {
        serverLog(LL_NOTICE, "rocksdb.block-cache-usage = %s", info);
        zlibc_free(info);
    }

    info = rocksdb_property_value(rockdb, "rocksdb.estimate-table-readers-mem");
    if (info) {
        serverLog(LL_NOTICE, "rocksdb.estimate-table-readers-mem = %s", info);
        zlibc_free(info);
    }

    info = rocksdb_property_value(rockdb, "rocksdb.cur-size-all-mem-tables");
    if (info) {
        serverLog(LL_NOTICE, "rocksdb.cur-size-all-mem-tables = %s", info);
        zlibc_free(info);
    }

    info = rocksdb_property_value(rockdb, "rocksdb.block-cache-pinned-usage");
    if (info) {
        serverLog(LL_NOTICE, "rocksdb.block-cache-pinned-usage = %s", info);
        zlibc_free(info);
    }
}

int debug_set_string_key_rock(uint8_t dbid, sds key) {
    redisDb *db = server.db + dbid;
    dictEntry *de = dictFind(db->dict, key);
    if (!de) return 1;
    robj *val = dictGetVal(de);
    serverAssert(val);
    if (val->type != OBJ_STRING) return 2;
    if (!(val->encoding == OBJ_ENCODING_RAW || val->encoding == OBJ_ENCODING_EMBSTR)) return 3;
    if (val == shared.keyRockVal) return 4;

    dictSetVal(db->dict, de, shared.keyRockVal);
    ++db->stat_key_str_rockval_cnt;
    addRockWriteTaskOfString(dbid, key, val->ptr);
    decrRefCount(val);
    return 0;
}

void debugRockCommand(client *c) {
    sds flag = c->argv[1]->ptr;

    if (strcasecmp(flag, "setstr") == 0) {
        if (c->argc != 3) {
            addReplyError(c, "debugrock setstr <key_name>");
            return;
        }

        if (!c->name || !c->name->ptr || strcasecmp(c->name->ptr, "_debug_") != 0) {
            addReplyError(c, "debugrock setstr must be client with name _debug_");
            return;
        }

        int ret = debug_set_string_key_rock(c->db->id, c->argv[2]->ptr);

        if (ret == 1) {
            addReplyError(c, "can not find the key to set rock value");
            return;            
        }
        if (ret == 2) {
            addReplyError(c, "key found, but type is not OBJ_STRING");
            return;
        }
        if (ret == 3) {
            addReplyError(c, "key found but type is not OBJ_ENCODING_RAW or OBJ_ENCODING_EMBSTR");
            return;
        }

        if (ret == 4) {
            addReplyError(c, "key found, but the value has already been shared.keyRockVal");
            return;
        }
    
    } else if (strcasecmp(flag, "setziplist") == 0) {
        if (c->argc != 3) {
            addReplyError(c, "debugrock setziplist <key_name>");
            return;
        }

        if (!c->name || !c->name->ptr || strcasecmp(c->name->ptr, "_debug_") != 0) {
            addReplyError(c, "debugrock setziplist must be client with name _debug_");
            return;
        }

        robj *key = c->argv[2];
        dictEntry *de = dictFind(c->db->dict, key->ptr);
        if (!de) {
            addReplyError(c, "can not find the key to set rock value");
            return;            
        }
        robj *val = dictGetVal(de);
        serverAssert(val);
        if (val->type != OBJ_HASH) {
            addReplyError(c, "key found, but type is not OBJ_HASH");
            return;
        }
        if (val->encoding != OBJ_ENCODING_ZIPLIST) {
            addReplyError(c, "key found but type is not OBJ_ENCODING_ZIPLIST");
            return;
        }
        if (val->refcount == OBJ_SHARED_REFCOUNT) {
            addReplyErrorFormat(c, "key found type conrrect, but the object is already shared, val == shared.ziplistRockVal is %s",
                                val == shared.ziplistRockVal ? "yes" : "no");
            return;
        }

        // dictSetVal(c->db->dict, de, shared.ziplistRockVal);
        dictGetVal(de) = shared.ziplistRockVal;
        ++c->db->stat_key_ziplist_rockval_cnt;
        unsigned char *zl = val->ptr;
        addRockWriteTaskOfZiplist(c->db->id, key->ptr, zl);
        decrRefCount(val);

    } else if (strcasecmp(flag, "mem") == 0) {
        report_rocksdb_mem_usage();

    } else {
        addReplyError(c, "wrong flag for debugrock!");
        return;
    }

    addReplyBulk(c,c->argv[0]);
}

/* When command be executed, it will chck the arguments for all the keys which value is in RocksDB.
 * If value in RocksDB, it will set rockKeyNumber > 0 and add a task for another thread to read from RocksDB
 * Task will be done in async mode 
 * NOTE1: the client c could by virtual client 
 * If the caller is for stream write, is_write_stream != 0 and we add task to both read_candidates and stream_wait_keys
 * NOTE2: actually checkAndSetRockKeyNumber() is belonged to read catagory, 
 *       so check the following read catagory after write catagory for more details */
void checkAndSetRockKeyNumber(client *c, const int is_stream_write) {
    // Because stream write is seriable
    if (is_stream_write) serverAssert(dictSize(stream_wait_rock_keys) == 0);

	serverAssert(c);
	serverAssert(c->streamWriting != STREAM_WRITE_WAITING);
	serverAssert(c->rockKeyNumber == 0);

    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);
    serverAssert(cmd);

    // the command does not need to check rock key, e.g., set <key> <val>
    if (!cmd->rock_proc) return;

    list *rock_keys = cmd->rock_proc(c);    // NOTE: rock_keys is allocated by cmd->rock_proc()
    if (!rock_keys) return;

    // cmd->rock_proc() has allocated memory resouce for the list of rock_keys 
    // and the sds keys in the list
    c->rockKeyNumber = listLength(rock_keys);
    serverAssert(c->rockKeyNumber > 0);

    lockRockRead();

    uint64_t client_id = c->id;
    listIter li;
    listNode *ln;
    int need_free_key;
    sds rock_key;
    sds saved_rock_key;
    dictEntry *de;
    list *new_clientids;
    list *saved_clientids;
    int dict_add_res;
    listRewind(rock_keys, &li);
    while ((ln = listNext(&li))) {
        need_free_key = 1;
        rock_key = listNodeValue(ln);
        de = dictFind(read_rock_key_candidates, rock_key);

        if (!de) {
            new_clientids = listCreate();
            listAddNodeTail(new_clientids, (void *)client_id);
            dictAdd(read_rock_key_candidates, rock_key, new_clientids);
            need_free_key = 0;      // read_rock_key_candidates own the rock_key now

            if (is_stream_write) {
                dict_add_res = dictAdd(stream_wait_rock_keys, rock_key, NULL);  
                serverAssert(dict_add_res == DICT_OK);  // must be a new key in stream_wait_rock_keys
            }   

        } else {
            saved_clientids = dictGetVal(de);
            listAddNodeTail(saved_clientids, (void *)client_id);

            if (is_stream_write) {
                // NOTE: key in stream_wait_rock_keys must point to the same object in read_rock_key_candidates
                saved_rock_key = dictGetKey(de);
                // NOTE: maybe add the same key serveral times, but it does not matter
                dictAdd(stream_wait_rock_keys, saved_rock_key, NULL);
            }
        }

        if (need_free_key) sdsfree(rock_key);
    }

    unlockRockRead();

    // clear list but not the keys in the list.
    // NOTE: some rock keys are owned now by read_rock_key_candidates
    listRelease(rock_keys);
}

/* allocate memory resouce by return sds. 
 * It is for string type with encoding of string, i.e., OBJ_ENCODING_RAW and OBJ_ENCODING_EMBSTR */
sds encode_rock_key_for_string(const uint8_t dbid, sds const string_key) {
    // we need allocate new memory for rock_key, 
    // because it may be transfered the owner to async-read_rock_key_candidates
    sds rock_key;
    uint8_t type = ROCK_STRING_TYPE;
    rock_key = sdsnewlen(&type, 1);
    rock_key = sdscatlen(rock_key, &dbid, 1);
    rock_key = sdscatlen(rock_key, string_key, sdslen(string_key));
    return rock_key;
}

/* allocate memory resource by resturn sds 
 * It is for hash type with encoding ziplist */
sds encode_rock_key_for_ziplist(const uint8_t dbid, sds const string_key) {
    sds rock_key;
    uint8_t type = ROCK_ZIPLIST_TYPE;
    rock_key = sdsnewlen(&type, 1);
    rock_key = sdscatlen(rock_key, &dbid, 1);
    rock_key = sdscatlen(rock_key, string_key, sdslen(string_key));
    return rock_key;
}

/* allocate memory resouce by return sds 
 * It is for hash with hash encoding, the rock_key has one more property as field */
sds encode_rock_key_for_hash(const uint8_t dbid, sds const key, sds const field) {
    // we need allocate new memory for rock_key
    // because it may be transfered the owner to async-read_rock_key_candidates
    sds rock_key;
    uint8_t type = ROCK_HASH_TYPE;
    rock_key = sdsnewlen(&type, 1);
    rock_key = sdscatlen(rock_key, &dbid, 1);
    uint32_t key_len = intrev32ifbe(sdslen(key));
    rock_key = sdscatlen(rock_key, &key_len, sizeof(key_len));
    rock_key = sdscatlen(rock_key, key, sdslen(key));
    rock_key = sdscatlen(rock_key, field, sdslen(field));
    return rock_key;
}

/* no memory allocation */
static void decode_rock_key(sds const rock_key, uint8_t *type, uint8_t *dbid, char **key, size_t *key_sz) {
    serverAssert(sdslen(rock_key) >= 2);
    *type = rock_key[0];
    serverAssert(*type == ROCK_STRING_TYPE || *type == ROCK_HASH_TYPE || *type == ROCK_ZIPLIST_TYPE);
    *dbid = rock_key[1];
    serverAssert(*dbid < server.dbnum);
    *key = rock_key+2;
    *key_sz = sdslen(rock_key) - 2;
}

/*                                 */
/*                                 */
/* The following is about write    */
/*                                 */
/*                                 */

void addRockWriteTaskOfString(uint8_t dbid, sds key, sds val) {
    sds rock_key, copy_val;
    struct WriteTask *task;

    // the resource will be reclaimed in pickWriteTasksInWriteThread()
    task = zmalloc(sizeof(*task));
    rock_key = encode_rock_key_for_string(dbid, key);
    copy_val = sdsdup(val);

    task->rock_key = rock_key;
    task->val = copy_val;

    lockRockWrite();
    listAddNodeTail(write_queue, task);
    unlockRockWrite();

    // modify db->str_zl_norock_keys
    redisDb *db = server.db + dbid;
    int ret = dictDelete(db->str_zl_norock_keys, key);
    serverAssert(ret == DICT_OK);
}

void addRockWriteTaskOfZiplist(uint8_t dbid, sds key, unsigned char *zl) {
    sds rock_key, copy_val;
    struct WriteTask *task;

    // the resource will be reclaimed in pickWriteTasksInWriteThread()
    task = zmalloc(sizeof(*task));
    rock_key = encode_rock_key_for_ziplist(dbid, key);
    size_t sz = ziplistBlobLen(zl);
    copy_val = sdsnewlen(zl, sz);

    task->rock_key = rock_key;
    task->val = copy_val;

    lockRockWrite();
    listAddNodeTail(write_queue, task);
    unlockRockWrite(); 

    // modify db->str_zl_norock_keys
    redisDb *db = server.db + dbid;
    int ret = dictDelete(db->str_zl_norock_keys, key);
    serverAssert(ret == DICT_OK);
}

void addRockWriteTaskOfHash(uint8_t dbid, sds key, sds field, sds val) {
    sds rock_key, copy_val;
    struct WriteTask *task;
    
    // the resource will be reclaimed in pickWriteTasksInWriteThread()
    task = zmalloc(sizeof(*task));
    rock_key = encode_rock_key_for_hash(dbid, key, field);
    copy_val = sdsdup(val);

    task->rock_key = rock_key;
    task->val = copy_val;

    lockRockWrite();
    listAddNodeTail(write_queue, task);
    unlockRockWrite();
}

/* First we remove the tasks of the number of before_pick_cnt which are regards as consumed in write_queue. 
 * Then we pick some tasks up to the limit of ROCK_WRITE_PICK_MAX_LEN and save them in the tasks array
 * Return: the pick number  */   
static size_t pickWriteTasksInWriteThread(struct WriteTask **tasks, const size_t before_pick_cnt) {
    serverAssert(before_pick_cnt <= ROCK_WRITE_PICK_MAX_LEN);

    listIter li;
    listNode *ln;
    struct WriteTask* to_free_tasks[ROCK_WRITE_PICK_MAX_LEN];

    lockRockWrite();

    // first remove the consumed elements in write_queue
    if (before_pick_cnt) {
        serverAssert(listLength(write_queue) >= before_pick_cnt);
        listRewind(write_queue, &li);
        for (size_t i = 0; i < before_pick_cnt; ++i) {
            ln = listNext(&li);
            struct WriteTask *task = listNodeValue(ln);
            to_free_tasks[i] = task;
            // release resource of the element of write_queue, 
            // but not the WriteTask resouce in the element
            listDelNode(write_queue, ln);   
        }
    }

    size_t cnt = 0;
    listRewind(write_queue, &li);
    while ((ln = listNext(&li)) && cnt < ROCK_WRITE_PICK_MAX_LEN) {
        tasks[cnt] = listNodeValue(ln);
        ++cnt;        
    }

    unlockRockWrite();

    // release WriteTask resource out of lock for performance
    struct WriteTask *to_free;
    for (size_t i = 0; i < before_pick_cnt; ++i) {
        to_free = to_free_tasks[i];
        serverAssert(to_free && to_free->rock_key && to_free->val);
        sdsfree(to_free->rock_key);
        sdsfree(to_free->val);
        zfree(to_free);
    }

    return cnt;
}

static void writeTasksToRocksdbInWriteThread(const size_t cnt, struct WriteTask **tasks) {
    rocksdb_writebatch_t *batch = rocksdb_writebatch_create();
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_disable_WAL(writeoptions, 1);
    char *err = NULL;

    for (size_t i = 0; i < cnt; ++i) {
        sds rock_key = tasks[i]->rock_key;
        sds val = tasks[i]->val;
        rocksdb_writebatch_put(batch, rock_key, sdslen(rock_key), val, sdslen(val));
    }
    rocksdb_write(rockdb, writeoptions, batch, &err);
    if (err) {
        serverLog(LL_WARNING, "writeTasksToRocksdbInWriteThread() failed reason = %s", err);
        exit(1);
    }

    rocksdb_writeoptions_destroy(writeoptions);
    rocksdb_writebatch_destroy(batch);
}

/* this is the write thread entrance. Here we init rocksdb environment 
 * and then start a forever loop to write value to Rocksdb and deal with error */
static void* entryInWriteThread(void *arg) {
    UNUSED(arg);

    uint sleepMicro = START_SLEEP_MICRO;
    struct WriteTask* tasks[ROCK_WRITE_PICK_MAX_LEN];
    size_t pick_cnt = 0;

    while(1) {
        pick_cnt = pickWriteTasksInWriteThread(tasks, pick_cnt);
        if (pick_cnt) {
            sleepMicro = START_SLEEP_MICRO;
            writeTasksToRocksdbInWriteThread(pick_cnt, tasks);
            continue;
        }

        usleep(sleepMicro);
        sleepMicro <<= 1;        // double sleep time
        if (sleepMicro > MAX_SLEEP_MICRO) 
            sleepMicro = MAX_SLEEP_MICRO;
    }

    return NULL;
}

static int unlink_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf)
{
    UNUSED(sb);
    UNUSED(typeflag);
    UNUSED(ftwbuf);

    int rv = remove(fpath);

    if (rv)
        perror(fpath);

    return rv;
}

static void rek_mkdir(char *path) {
    char *sep = strrchr(path, '/');
    if(sep != NULL) {
        *sep = 0;
        rek_mkdir(path);
        *sep = '/';
    }
    if(sep != NULL && mkdir(path, 0777) && errno != EEXIST)
        serverLog(LL_WARNING, "error while trying to create folder = %s", path); 
}

static void initRocksdb() {
    serverLog(LL_NOTICE, "start to delete old RocksDB folder = %s ...", server.bunny_rockdb_path);
    // We need to remove the whole RocksDB folder like rm -rf <rocksdb_folder>
    nftw(server.bunny_rockdb_path, unlink_cb, 64, FTW_DEPTH | FTW_PHYS);
    serverLog(LL_NOTICE, "finish removal of the whole folder = %s", server.bunny_rockdb_path);
    // mkdir 
    mode_t mode = 0777;
    if (mkdir(server.bunny_rockdb_path, mode)) {
        if (errno == ENOENT) {
            // folder not exist
            rek_mkdir(server.bunny_rockdb_path);
        } else {
            serverLog(LL_WARNING, "Can not mkdir %s with mode 777, errno = %d",  server.bunny_rockdb_path, errno);
            exit(1);
        }
    }

    long cpus = sysconf(_SC_NPROCESSORS_ONLN);
    rocksdb_options_t *options = rocksdb_options_create();

    // Set # of online cores
    rocksdb_options_increase_parallelism(options, (int)(cpus));
    rocksdb_options_optimize_level_style_compaction(options, 0); 
    // create the DB if it's not already present
    rocksdb_options_set_create_if_missing(options, 1);
    // file size
    rocksdb_options_set_target_file_size_base(options, 4<<20);
    // memtable
    rocksdb_options_set_write_buffer_size(options, 32<<20);     // 32M memtable size
    rocksdb_options_set_max_write_buffer_number(options, 4);    // memtable number
    // WAL
    // rocksdb_options_set_manual_wal_flush(options, 1);    // current RocksDB API 6.20.3 not support
    // compaction (using Universal Compaction)
    rocksdb_options_set_compaction_style(options, rocksdb_universal_compaction);
    rocksdb_options_set_num_levels(options, ROCKSDB_LEVEL_NUM);   
    rocksdb_options_set_level0_file_num_compaction_trigger(options, 4);
    // set each level compression types (reference RocksDB API of compression_type.h)
    int compression_level_types[ROCKSDB_LEVEL_NUM];
    for (int i = 0; i < ROCKSDB_LEVEL_NUM; ++i) {
        if (i == 0 || i == 1) {
            compression_level_types[i] = 0x0;   // kNoCompression
        } else {
            compression_level_types[i] = 0x04;      // kLZ4Compression
        }
    }
    rocksdb_options_set_compression_per_level(options, compression_level_types, ROCKSDB_LEVEL_NUM);
    // table options
    rocksdb_options_set_max_open_files(options, 1024);      // if default is -1, no limit, and too many open files consume memory
    rocksdb_options_set_table_cache_numshardbits(options, 4);        // shards for table cache
    rocksdb_block_based_table_options_t *table_options = rocksdb_block_based_options_create();
    // block size (Although the RocksDB website recommend 16K-32K in production), we need a test for 4K or 8K
    rocksdb_block_based_options_set_block_size(table_options, 4<<10);
    // block cache
    rocksdb_cache_t *lru_cache = rocksdb_cache_create_lru(256<<20);        // 256M lru cache
    rocksdb_block_based_options_set_block_cache(table_options, lru_cache);
    // index in cache and partitioned index filter (https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters)
    rocksdb_block_based_options_set_index_type(table_options, rocksdb_block_based_table_index_type_two_level_index_search);
    rocksdb_block_based_options_set_partition_filters(table_options, 1);
    rocksdb_block_based_options_set_metadata_block_size(table_options, 4<<10);
    // filter and index in block cache to save memory
    rocksdb_block_based_options_set_cache_index_and_filter_blocks(table_options, 1);    
    rocksdb_block_based_options_set_pin_top_level_index_and_filter(table_options, 1);
    rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(table_options, 1);
    // NOTE: we use universal compaction, so not set pin_l0_filter_and_index_blocks_in_cache
    // rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(table_options, 1);

    // bloom filter
    rocksdb_filterpolicy_t *bloom = rocksdb_filterpolicy_create_bloom_full(10);
    rocksdb_block_based_options_set_filter_policy(table_options, bloom);
    // need invest, maybe mix with rocksdb_options_optimize_level_style_compaction()
    // rocksdb_options_set_max_background_jobs(options, 3);     

    rocksdb_options_set_block_based_table_factory(options, table_options);

    // open DB
    char *err = NULL;
    rockdb = rocksdb_open(options, server.bunny_rockdb_path, &err);
    if (err) {
        serverLog(LL_WARNING, "initRocksdb() failed reason = %s", err);
        exit(1);
    }

    // clean up
    // rocksdb_filterpolicy_destroy(bloom);   
    rocksdb_cache_destroy(lru_cache);
    rocksdb_block_based_options_destroy(table_options);    
    rocksdb_options_destroy(options);
}

/* init: 1. open RocksDB; 2. write_queue; 3. writeLock of spin lock; 4. start write thread */ 
void initRockWrite() {
    initRocksdb();
    write_queue = listCreate();

   int spin_init_res = pthread_spin_init(&writeLock, 0);
    if (spin_init_res != 0)
        serverPanic("Can not init rock write spin lock, error code = %d", spin_init_res);

    pthread_t write_thread;
    if (pthread_create(&write_thread, NULL, entryInWriteThread, NULL) != 0) 
        serverPanic("Unable to create a rock write thread.");
}

void closeRockdb() {
    rocksdb_close(rockdb);
}

const char* getRockdbPath() {
    return server.bunny_rockdb_path;
}

/*                                 */
/*                                 */
/* The following is about read     */
/*                                 */
/*                                 */

/* return C_ERR if on_fly_task_cnt has not been consumed by main thread, 
 * return C_OK and the number of picking read tasks in pick_cnt
 * 
 * We will use sds array for pick_rock_keys for no memory allocation 
 * to perform good in thread contention */
static int pickReadTasksInReadThread(size_t *pick_cnt, sds *pick_rock_keys) {
    lockRockRead();
    
    if (on_fly_task_cnt) {
        // if main thread has not consumed the on_fly_task_cnt task
        // Read thread needs to wait
        unlockRockRead();
        return C_ERR;
    }

    size_t cnt = 0;
    if (dictSize(read_rock_key_candidates)) {
        sds rock_key;
        dictIterator *di;
        dictEntry *de;
        if (dictSize(stream_wait_rock_keys)) {
            // the stream_waiting_keys has higher priority
            di = dictGetIterator(stream_wait_rock_keys);
        } else {
            di = dictGetIterator(read_rock_key_candidates);
        }
        while ((de = dictNext(di)) && cnt < MAX_READ_TASK_NUM) {
            rock_key = dictGetKey(de);      // NOTE: rock_key is owned by read_rock_key_candidates
            pick_rock_keys[cnt] = rock_key;  
            on_fly_rock_keys[cnt] = rock_key;
            ++cnt;
        }
        dictReleaseIterator(di);
    
        // It indicates there are some read tasks on fly and avoids repeated picking for the same keys
        // When main thread consume the read-done tasks it will set on_fly_task_cnt to zero
        on_fly_task_cnt = cnt;  
    }    

    unlockRockRead();

    *pick_cnt = cnt;
    return C_OK;
}

/* signal main thread which will call rockReadSignalHandler() */
static void signalMainThreadByPipeInReadThread() {
    char tempBuf[1] = "a";
    size_t n = write(server.rock_pipe_write, tempBuf, 1);
    serverAssert(n == 1);
}

/* check values from write queue if key match. Caller need set all vals to NULL beforehand */
static void readFromWriteQueueFirstInReadThread(const size_t task_cnt, sds const* const task_rock_keys,
                                                sds *vals) {
    listIter li;
    listNode *ln;
    sds read_rock_key;
    size_t write_queue_len;
    struct WriteTask *write_task;
    sds write_rock_key;
    sds copy_val;

    static int first_avoid_warning = 1;

    lockRockWrite();

    write_queue_len = listLength(write_queue);
    for (size_t i = 0; i < task_cnt; ++i) {
        read_rock_key = task_rock_keys[i];
        
        // We assume the write_queue is short enough
        if (write_queue_len > ROCK_WRITE_QUEUE_TOO_LONG) {
            int consume_startup;
            atomicGet(kafkaStartupConsumeFinish, consume_startup);
            // In startup, we do not log warning
            if (consume_startup == CONSUMER_STARTUP_OPEN_TO_CLIENTS) {
                // we avoid the first warning because it may be the remain work length just when startup is over
                if (!first_avoid_warning)      
                    serverLog(LL_WARNING, "write queue of Rock is too long, len = %lu", write_queue_len);

                if (write_queue_len == task_cnt) first_avoid_warning = 0;
            }
        }

        listRewind(write_queue, &li);
        while ((ln = listNext(&li))) {
            write_task = listNodeValue(ln);
            write_rock_key = write_task->rock_key;
            if (sdslen(read_rock_key) == sdslen(write_rock_key) && 
                sdscmp(read_rock_key, write_rock_key) == 0) {
                // The val in write queue will be destroyed in pickWriteTasksInWriteThread().
                // So we need a copy of the val.
                copy_val = sdsdup(write_task->val);   
                // the copy_val will be transfered to on_fly_vals
                // and be reclainmed in rockReadSignalHandler()
                vals[i] = copy_val;
            }
        }
    }

    unlockRockWrite();
}

/* For real read, we need to check the write_queue first because write and read are all async 
 * check addRockWriteTaskOfString() for more details */
static void doReadTasksInReadThread(const size_t task_cnt, sds const* const task_rock_keys) {
    serverAssert(task_cnt > 0 && task_cnt <= MAX_READ_TASK_NUM);

    // in the end, all values from wirte queue and RocksDB are in vals
    sds vals[MAX_READ_TASK_NUM];        
    for (size_t i = 0; i < task_cnt; ++i) {
        vals[i] = NULL;
    }

    // first, read from write_queue
    readFromWriteQueueFirstInReadThread(task_cnt, task_rock_keys, vals);

    // filter the unknown value to prepare for RocksDB
    size_t rockdb_read_cnt = 0;
    size_t indexs[MAX_READ_TASK_NUM];
    sds rockdb_keys[MAX_READ_TASK_NUM];
    size_t rockdb_key_sizes[MAX_READ_TASK_NUM];
    for (size_t i = 0; i < task_cnt; ++i) {
        if (!vals[i]) {
            // If vals[i] are not read from write queue, we need read the values of them from RocksDB
            indexs[rockdb_read_cnt] = i;
            rockdb_keys[rockdb_read_cnt] = task_rock_keys[i];
            rockdb_key_sizes[rockdb_read_cnt] = sdslen(task_rock_keys[i]);
            ++rockdb_read_cnt;
        }
    }

    // read the unknow values from RocksDB using RocksDB MultiGet
    char* rockdb_vals[MAX_READ_TASK_NUM];
    size_t rockdb_val_sizes[MAX_READ_TASK_NUM];
    char* errs[MAX_READ_TASK_NUM];
    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    rocksdb_multi_get(rockdb, readoptions, rockdb_read_cnt, 
                      (const char* const *)rockdb_keys, rockdb_key_sizes, rockdb_vals, rockdb_val_sizes, errs);
    rocksdb_readoptions_destroy(readoptions);

    // Join values from write_queue and RocksDB
    for (size_t i = 0; i < rockdb_read_cnt; ++i) {
        if (errs[i]) {
            serverLog(LL_WARNING, "doReadTasksInReadThread() reading from RocksDB failed, err = %s, key = %s",
                      errs[i], rockdb_keys[i]+2);
            exit(1);
        }

        size_t index = indexs[i];
        serverAssert(vals[index] == NULL);
        if (rockdb_vals[i] == NULL) {
            // not found
            vals[index] = val_not_found;
        } else {
            // We need sds in vals, not char* from RocksDB API. 
            // So we copy it. The vals will be transfered to on_fly_vals and be reclained in rockReadSignalHandler()
            vals[index] = sdsnewlen(rockdb_vals[i], rockdb_val_sizes[i]);
            // free the malloc()ed resource made by RocksDB API of rocksdb_multi_get()
            zlibc_free(rockdb_vals[i]);       
        }
    }

    // Finally, update the on_fly data by tranfering all vals to on_fly_vals
    lockRockRead();
    serverAssert(on_fly_task_cnt == task_cnt);
    for (size_t i = 0; i < task_cnt; ++i) {
        serverAssert(on_fly_vals[i] == NULL && vals[i]);
        on_fly_vals[i] = vals[i];
    }
    unlockRockRead();

    // for debug
    if (server._debug_) {
        for (size_t i = 0; i < task_cnt; ++i) {
            sds val = vals[i];
            if (sdslen(val) == 3 && val[0] == 'v' && val[1] == 'a' && val[2] == 'l') {
                serverLog(LL_WARNING, "debug: sleep for value == val to sleep for 10 seconds");
                usleep(10*1000*1000);
                serverLog(LL_WARNING, "debug: sleep for value == val is over");
            }
        }
    }

    // Do not forget to signal main thread to call rockReadSignalHandler()
    signalMainThreadByPipeInReadThread();
}

static void* entryInReadThread(void *arg) {
    UNUSED(arg);

    sds pick_rock_keys[MAX_READ_TASK_NUM];
    size_t pick_cnt;

    uint sleepMicro = START_SLEEP_MICRO;
    while (1) {
        // If we want to do some test, we can use the following code to simulate
        // the slow rocksDB loanding process
        // serverLog(LL_WARNING, "read thread sleep for 10 seconds");
        // usleep(10*1000*1000);

        if (pickReadTasksInReadThread(&pick_cnt, pick_rock_keys) == C_ERR) {
            // We sleep a short while waiting for main thread to consume on_fly_vals
            usleep(START_SLEEP_MICRO);
            continue;
        }

        if (pick_cnt == 0) {
            usleep(sleepMicro);
            sleepMicro <<= 1;        // double sleep time
            if (sleepMicro > MAX_SLEEP_MICRO) 
                sleepMicro = MAX_SLEEP_MICRO;
        } else {
            doReadTasksInReadThread(pick_cnt, pick_rock_keys);
            sleepMicro = START_SLEEP_MICRO;
        }
    }

    return NULL;
}

/* NOTE: will allocate memory for *hash_key and *hash_field even it is zero-length
 * About the encode, check encode_rock_key_for_hash() */
static void decodeKeyToHashKeyAndHashField(sds const key, sds *hash_key, sds *hash_field) {
    size_t key_len = sdslen(key);
    size_t cnt = 0;
    char const *p = key;

    uint32_t hash_key_len = intrev32ifbe(*((uint32_t*)p));
    cnt += sizeof(uint32_t);
    p += sizeof(uint32_t);
    serverAssert(cnt <= key_len);
    serverAssert(cnt + hash_key_len <= key_len);

    sds allocate_key = sdsnewlen(p, hash_key_len);
    p += hash_key_len;
    cnt += hash_key_len;
    sds allocate_field = sdsnewlen(p, key_len-cnt);

    *hash_key = allocate_key;
    *hash_field = allocate_field;
}

/* Recover the shared. to the real value */
static void recover_val(const uint8_t type, const uint8_t dbid, sds const key, sds const val) {
    if (type == ROCK_STRING_TYPE) {
        redisDb *db = server.db+dbid;
        dict *dict = db->dict;
        dictEntry *de = dictFind(dict, key);
        serverAssert(de);
        robj *dbval = dictGetVal(de);
        if (dbval != shared.keyRockVal) {
            serverLog(LL_WARNING, "recover_val() found the dbval is not shared.keyRockVal, key = %s", key);
            exit(1);
        }
        robj *recover = createStringObject(val, sdslen(val));
        // dictSetVal(dict, de, recover);
        dictGetVal(de) = recover;
        serverAssert(db->stat_key_str_rockval_cnt);
        --db->stat_key_str_rockval_cnt;
        int ret = dictAdd(db->str_zl_norock_keys, dictGetKey(de), 0); // NOTE: not use key, use the key in db->dict
        serverAssert(ret == DICT_OK);

    } else if (type == ROCK_ZIPLIST_TYPE) {
        redisDb *db = server.db+dbid;
        dict *dict = db->dict;
        dictEntry *de = dictFind(dict, key);
        serverAssert(de);
        robj *dbval = dictGetVal(de);
        if (dbval != shared.ziplistRockVal) {
            serverLog(LL_WARNING, "recover_val() found the dbval is not shared.ziplistRockVal, key = %s", key);
            exit(1);
        }
        // reference t_hash.c hashTypeDup()
        size_t sz = sdslen(val);
        unsigned char *zl = zmalloc(sz);
        memcpy(zl, val, sz);
        robj *recover = createObject(OBJ_HASH, zl);
        recover->encoding = OBJ_ENCODING_ZIPLIST;
        dictGetVal(de) = recover;
        serverAssert(db->stat_key_ziplist_rockval_cnt);
        --db->stat_key_ziplist_rockval_cnt;
        int ret = dictAdd(db->str_zl_norock_keys, dictGetKey(de), 0); // NOTE: not use key, use the key in db->dict  
        serverAssert(ret == DICT_OK);

    } else {
        serverAssert(type == ROCK_HASH_TYPE);
        // decode the combined key to hash_key and hash_field
        sds hash_key, hash_field;
        decodeKeyToHashKeyAndHashField(key, &hash_key, &hash_field);

        // find the object in db
        redisDb *db = server.db+dbid;
        dict *dict_db = db->dict;
        dictEntry *de_db = dictFind(dict_db, hash_key);
        serverAssert(de_db);
        robj *o = dictGetVal(de_db);
        if (o->type != OBJ_HASH && o->encoding != OBJ_ENCODING_HT) {
            serverLog(LL_WARNING, "recover_val() found the object is not right hash, key = %s", hash_key);
            exit(1);
        }

        // find the de in hash and recover
        dict *dict_hash = o->ptr;
        dictEntry *de_hash = dictFind(dict_hash, hash_field);
        serverAssert(de_hash);
        if (dictGetVal(de_hash) != shared.hashRockVal) {
            serverLog(LL_WARNING, "recover_val(), the val of hash is not shared.hashRockVal, key = %s, field = %s, val = %s",
                      hash_key, hash_field, (sds)dictGetVal(de_hash));
            exit(1);
        }
        sds recover = sdsdup(val);
        dictGetVal(de_hash) = recover;

        // update the stat if possible because server.evict_hash_candidates maybe change
        evictHash *evict_hash = lookupEvictOfHash(dbid, hash_key);
        if (evict_hash) {
            serverAssert(evict_hash->rock_cnt > 0);
            --evict_hash->rock_cnt;
            // NOTE: use the same field of pure hash
            int ret = dictAdd(evict_hash->no_rocks, dictGetKey(de_hash), 0);     
            serverAssert(ret == DICT_OK);
        }

        sdsfree(hash_key);
        sdsfree(hash_field);
    }
}

/* When read thread finish the read tasks, it sets the return values 
 * in on_fly_vals by doReadTasksInReadThread() and signal the main thread. 
 * Here is the signal handler of main thread. 
 * It will recover the vals and reclaim memory resource */
static void rockReadSignalHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    UNUSED(fd);

    // clear pipe signal
    char tmpUseBuf[1];
    size_t n = read(server.rock_pipe_read, tmpUseBuf, 1);     
    serverAssert(n == 1);

    // We must use another list to store all associated client ids. 
    // Because when the client id is resumed to go on, 
    // it will call spin lock of read in checkAndSetRockKeyNumber().
    // The spin lock can not re-entered.
    list *all_client_ids = listCreate();
    listIter li;
    listNode *ln;
    sds rock_key;
    sds val;
    sds sds_key;

    uint8_t type;
    uint8_t dbid;
    char *c_key;
    size_t c_key_sz;

    lockRockRead();

    serverAssert(on_fly_task_cnt);
    for (size_t i = 0; i < on_fly_task_cnt; ++i) {
        rock_key = on_fly_rock_keys[i];
        serverAssert(rock_key);

        // Decode the rock_key. NOTE: decode_rock_key() will not allocate resource
        decode_rock_key(rock_key, &type, &dbid, &c_key, &c_key_sz);
        val = on_fly_vals[i];
        if (val == NULL || val == val_not_found) {
            // The real value must in write_queue or RocksDB
            serverLog(LL_WARNING, "Can not found the vaule in RocksDB for key = %s val is %s", 
                      c_key, val ? "val is not NULL" : "val is NULL");
            exit(1);
        }      

        // recover the real val
        sds_key = sdsnewlen(c_key, c_key_sz);
        recover_val(type, dbid, sds_key, val);
        sdsfree(sds_key);

        // deal with stream_wait_rock_keys and read_rock_key_candidates and reclaim the resource in them
        dictDelete(stream_wait_rock_keys, rock_key);        // rock_key maybe in stream_wait_rock_keys or not
        dictEntry *de = dictFind(read_rock_key_candidates, rock_key);
        serverAssert(de);
        list *client_ids = dictGetVal(de);
        serverAssert(listLength(client_ids));
        listRewind(client_ids, &li);
        while ((ln = listNext(&li))) {
            uint64_t client_id = (uint64_t)listNodeValue(ln);
            listAddNodeTail(all_client_ids, (void *)client_id);
        }
        // NOTE: dictDelete(read_rock_key_candidates) will reclaim memory resource 
        //       for the rock_key and the list of clientids. Check dictType readCandidatesDictType for more details.
        dictDelete(read_rock_key_candidates, rock_key);   

        // Reclaim the resource of the val. (The rock key has been reclaimed in the last statement) 
        // NOTE: It guaratees that the val is not val_not_found
        sdsfree(val);        
        on_fly_vals[i] = NULL;      // Do not need to set on_fly_keys[i] to be NULL
    }

    on_fly_task_cnt = 0;        // Let read thread go on. Chceck pickReadTasksInReadThread().

    unlockRockRead();

    // Resume all rocked client ids. Right now client id may be 
    // 1. virtual client id
    // 2. concrete client id 
    // 3. before-concrete client id (if the connection has been dropped because of async)
    uint64_t client_id;
    client *c;
    dictEntry *de;
    listRewind(all_client_ids, &li);
    int need_go_on_for_stream = 0;
    while ((ln = listNext(&li))) {
        client_id = (uint64_t)listNodeValue(ln);
        de = dictFind(server.clientIdTable, (const void*)client_id);

        if (de) {
            // concrete client id right now. Resume by processCommandAndResetClient()
            c = dictGetVal(de);
            serverAssert(c->rockKeyNumber);
            --c->rockKeyNumber;
            if (c->rockKeyNumber == 0) {
                int is_stream = server.streamCurrentClientId == client_id;
                checkAndSetRockKeyNumber(c, is_stream);
                if (c->rockKeyNumber == 0) {
                    int is_stream = c->id == server.streamCurrentClientId;
                    processCommandAndResetClient(c);
                    if (is_stream) {
                        need_go_on_for_stream = 1;
                    }     
                    // for Redis Pipe. We need read more
                    if (c->querybuf && sdslen(c->querybuf) > 0) 
                        processInputBuffer(c);
                }
            }
        } else {
            if (client_id == VIRTUAL_CLIENT_ID)
                // It must be from stream write because virtual client only execute stream write commands
                serverAssert(server.streamCurrentClientId == VIRTUAL_CLIENT_ID);   

            if (client_id != VIRTUAL_CLIENT_ID && server.streamCurrentClientId != client_id) {
                // If the client is of before-concrete client id and not from stream write,
                // we can skip execVirtualCommand(). 
                // E.g. The client reads some key with value in RocksDB but the connection is dropped
                //      before it commes here to be resumed for execVirtualCommand()   
            } else {
                // NOTE: If the client is before-concrete client id, 
                //       it guarantees the contex of before_concrete client has been switched to virtual context
                //       Check setVirtualContextFromConcreteClient() for more details
                c = server.virtual_client;
                serverAssert(c->rockKeyNumber);
                --c->rockKeyNumber;
                if (c->rockKeyNumber == 0) {
                    checkAndSetRockKeyNumber(c, 1);
                    if (c->rockKeyNumber == 0) {
                        execVirtualCommand();
                        need_go_on_for_stream = 1;
                    }
                }
            }
        }
    }

    // release the last memory resource
    listRelease(all_client_ids);

    if (need_go_on_for_stream)
        try_to_execute_stream_commands();
}

void initRockPipeAndRockRead() {
    pthread_t read_thread;
    int pipefds[2];
    read_rock_key_candidates = dictCreate(&readCandidatesDictType, NULL);
    stream_wait_rock_keys = dictCreate(&streamWaitKeyDictType, NULL);
    on_fly_task_cnt =0;
    val_not_found = sdsnew("Not found in Rocks");
    for (int i = 0; i < MAX_READ_TASK_NUM; ++i) {
        on_fly_vals[i] = NULL;      // no finished values for initialization
    }

   int spin_init_res = pthread_spin_init(&readLock, 0);
    if (spin_init_res != 0)
        serverPanic("Can not init rock read spin lock, error code = %d", spin_init_res);

    if (pipe(pipefds) == -1) 
        serverPanic("Can not create pipe for rock.");

    server.rock_pipe_read = pipefds[0];
    server.rock_pipe_write = pipefds[1];

    if (aeCreateFileEvent(server.el, server.rock_pipe_read, 
        AE_READABLE, rockReadSignalHandler,NULL) == AE_ERR) {
        serverPanic("Unrecoverable error creating server.rock_pipe file event.");
    }

    if (pthread_create(&read_thread, NULL, entryInReadThread, NULL) != 0) 
        serverPanic("Unable to create a rock write thread.");
}

/* when db delete by sync or async, we need update stat for rock 
 * 1. if string, update string for db.rock_stat and string_stat
 * 2. if hash with encoding ziplist, update ziplist for db.rock_stat and ziplist_stat
 * 3. if hash with encoding hash, try to remove it from evict_hash_candidates  
 * de is the dict entry of the db which will be delete */
void update_rock_stat_and_try_delete_evict_candidate_for_db_delete(redisDb *db, dictEntry* de) {
    serverAssert(db && de);

    sds key = dictGetKey(de);
    robj *o = dictGetVal(de);

    if (o->type == OBJ_STRING && (o->encoding == OBJ_ENCODING_RAW || o->encoding == OBJ_ENCODING_EMBSTR)) {
        serverAssert(db->stat_key_str_cnt);
        --db->stat_key_str_cnt;
        if (o == shared.keyRockVal) {
            serverAssert(db->stat_key_str_rockval_cnt);
            --db->stat_key_str_rockval_cnt;
        } else {
            int ret = dictDelete(db->str_zl_norock_keys, key);
            serverAssert(ret == DICT_OK);
        }

    } else if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_ZIPLIST) {
        serverAssert(db->stat_key_ziplist_cnt);
        --db->stat_key_ziplist_cnt;
        if (o == shared.ziplistRockVal) {
            serverAssert(db->stat_key_ziplist_rockval_cnt);
            --db->stat_key_ziplist_rockval_cnt;
        } else {
            int ret = dictDelete(db->str_zl_norock_keys, key);
            serverAssert(ret == DICT_OK);
        }

    } else if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT) {
        // sds combined = combine_dbid_key(db->id, key); 
        removeHashCandidate(db->id, key);
        // free_combine_dbid_key(combined);
    }
}

/* check the index argument of c to form rock_keys list 
 * if the type is pure hash, we return NULL because the caller does not care about the pure hash
 * for example: getset command will overwrite, and setbit will return failure for type checking */
list* genericGetOneKeyExcludePureHashForRock(client *c, int index) {
    uint8_t dbid = c->db->id;
    dict *dict_db = (server.db+dbid)->dict;
    sds key = c->argv[index]->ptr;
    dictEntry *de_db = dictFind(dict_db, key);
    if (!de_db) return NULL;

    robj *o = dictGetVal(de_db);

    if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT) return NULL;
    /*
        serverPanic("genericGetOneKeyExcludePureHashForRock() can not be called when pure hash, key = %s, cmd = %s", 
                    key, (sds)c->argv[0]->ptr);
    */

    if (!(o->type == OBJ_STRING || (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_ZIPLIST)))
        return NULL;

    if (o->type == OBJ_STRING) {
        if (o != shared.keyRockVal) return NULL;
        list *rock_keys = listCreate();
        sds rock_key = encode_rock_key_for_string(dbid, key);
        listAddNodeTail(rock_keys, rock_key);
        return rock_keys;
    } else {
        // ziplist
        return hGenericRockForZiplist(dbid, key, o);
    }
}

/* for hash type (and one key, key is the second argument). 
 * If encoding is ziplist, return from hGenericRockForZiplist().
 * otherwise, return all fields which are in Rock */ 
list* hGenericGetAllFieldForRock(client *c) {
    uint8_t dbid = c->db->id;
    sds key = c->argv[1]->ptr;
    dict *dict_db = (server.db+dbid)->dict;

    dictEntry *de_db = dictFind(dict_db, key);
    if (!de_db) return NULL;

    robj *o = dictGetVal(de_db);
    if (o->type != OBJ_HASH) {
        serverPanic("hGenericGetAllFieldForRock() type is not OBJ_HASH, key = %s", key);
    }

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        return hGenericRockForZiplist(dbid, key, o);
    } else {    
        serverAssert(o->encoding == OBJ_ENCODING_HT);
        list *rock_keys = NULL;
        dict *dict_hash = o->ptr;
        dictIterator *di = dictGetIterator(dict_hash);
        dictEntry *de;
        while ((de = dictNext(di))) {
            sds field = dictGetKey(de);
            sds val = dictGetVal(de);
            if (val == shared.hashRockVal) {
                if (rock_keys == NULL) rock_keys = listCreate();
                sds rock_key = encode_rock_key_for_hash(dbid, key, field);
                listAddNodeTail(rock_keys, rock_key);
            }
        }
        dictReleaseIterator(di);
        return rock_keys;
    }
}

/* key type could be string, ziplist or pure hash */
list* genericGetOneKeyForRock(client *c) {
    redisDb *db = server.db + c->db->id;
    sds key = c->argv[1]->ptr;
    
    dictEntry *de_db = dictFind(db->dict, key);
    if (!de_db) return NULL;

    robj *o = dictGetVal(de_db);

    if (o->type == OBJ_HASH) {
        return hGenericGetAllFieldForRock(c);
    } else {
        return genericGetOneKeyExcludePureHashForRock(c, 1);
    }
}

/* if encoding is ziplist, check whether the whole key is in Rock 
 * if encoding is hash, check whether the field value is in Rock */
list* hGenericGetOneFieldForRock(client *c) {
    uint8_t dbid = c->db->id;
    redisDb *db = server.db + dbid;
    dict *dict_db = db->dict;
    sds key = c->argv[1]->ptr;

    dictEntry *de_db = dictFind(dict_db, key);
    if (!de_db) return NULL;

    robj *o = dictGetVal(de_db);
    if (!o) return NULL;

    // if (o->type != OBJ_HASH) return NULL;
    serverAssert(o->type == OBJ_HASH);

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        // restor the whole fields for one key if ziplist
        return hGenericRockForZiplist(dbid, key, o);        
    } else {
        serverAssert(o->encoding == OBJ_ENCODING_HT);
        dict *dict_hash = o->ptr;
        sds field = c->argv[2]->ptr;
        dictEntry *de_hash = dictFind(dict_hash, field);
        if (!de_hash) return NULL;

        sds val = dictGetVal(de_hash);
        if (val != shared.hashRockVal) return NULL;

        list *rock_keys = listCreate();
        sds rock_key = encode_rock_key_for_hash(dbid, key, field);
        listAddNodeTail(rock_keys, rock_key);
        return rock_keys;   
    } 
}

list* hGenericRockForZiplist(uint8_t dbid, sds key, robj *o) {
    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    if (o != shared.ziplistRockVal) {
        return NULL;
    } else {
        list *rock_keys = listCreate();
        sds rock_key = encode_rock_key_for_ziplist(dbid, key);
        listAddNodeTail(rock_keys, rock_key);
        return rock_keys;
    }
}

/* if the key is ziplist in Rock, return the key
 * otherwise, return NULL
 * NOTE: it could be hash encdong and the some fields is in Rock, but it will return NULL,
 *       because the fields is to be overridden (hsetex) or just check the fiels (hexists), 
 *       we do not care about the rock value of pure hash */
list* hGenericGetOneKeyOfZiplistForRock(client *c) {
    uint8_t dbid = c->db->id;
    redisDb *db = server.db + dbid;
    dict *dict_db = db->dict;
    sds key = c->argv[1]->ptr;

    dictEntry *de_db = dictFind(dict_db, key);
    if (!de_db) return NULL;

    robj *o = dictGetVal(de_db);
    if (!o) return NULL;

    // if the object is string, we do not need to recover it
    // because the caller will fail the type checkings
    if (o->type == OBJ_STRING) return NULL;     

    serverAssert(o->type == OBJ_HASH);

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        return hGenericRockForZiplist(dbid, key, o);
    } else {
        return NULL;
    }
}

/* for Mainthread other module, e.g., rockevict cron job  */
size_t get_rock_write_queue_len() {
    size_t len;
    lockRockWrite();
    len = listLength(write_queue);
    unlockRockWrite();
    return len;
}