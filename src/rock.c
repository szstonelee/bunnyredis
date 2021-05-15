
#include "server.h"
#include "rock.h"
#include "networking.h"
#include "streamwrite.h"

#include "assert.h"
#include <rocksdb/c.h>

#define START_SLEEP_MICRO   16
#define MAX_SLEEP_MICRO     1024            // max sleep for 1 ms

#define ROCK_STRING_TYPE     0           // Rocksdb key is coded as dbid(uint8_t) + byte key of Redis string type
#define ROCK_HASH_TYPE       1           // Rocksdb key is coded as dbid(uint8_t) + (uint32_t) key_size + byte of key + byte of field

#define ROCK_WRITE_QUEUE_TOO_LONG   1<<10
#define ROCK_WRITE_PICK_MAX_LEN     8

// spin lock only run in Linux
static pthread_spinlock_t readLock;     
static pthread_spinlock_t writeLock;

rocksdb_t *rockdb;
const char bunny_rockdb_path[] = "/tmp/bunnyrocksdb";

struct WriteTask {
    int type;
    sds key;
    sds val;
};

list* write_queue;

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
    NULL,                       /* key compare */
    keyInRockDestructor,        /* key destructor, we stored the clientid in the key pointer */
    valAsListDestructor,        /* val destructor */
    NULL                        /* allow to expand */
};

/* key is sds in readCandidatesDictType. value is de* in readCandidatesDictType */
dictType streamWaitKeyDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    NULL,                       /* key compare */
    NULL,                       /* key destructor, we stored the clientid in the key pointer */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

// stream_wait_keys is a set. Set key is sds key which stream write is waiting for. 
// And the sds key share the same object in read_candidates. So does not need to be freed
dict *stream_wait_keys;     
// read_candidates is a dictionary. Dict key is sds key as reading task. 
// Dict val is a list of client id waiting for the key. 
// NOTE: the list could have duplicated client id. 
//       e.g., multi -> exec could leed to duplicated rock key with the same client.
dict *read_candidates;

#define MAX_READ_TASK_NUM   16
size_t on_fly_task_cnt;        // how may task for the read thraed to do
sds on_fly_keys[MAX_READ_TASK_NUM];
sds on_fly_vals[MAX_READ_TASK_NUM];        // if first element is NULL, it means no return values
sds val_not_found;

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

/*
static void debugPrintRockKeys(list *rock_keys) {
    listIter li;
    listNode *ln;
    listRewind(rock_keys, &li);
    while ((ln = listNext(&li))) {
        sds key = listNodeValue(ln);
        serverLog(LL_WARNING, "debugPrintRockKeys(), key = %s", key);
    }
}
*/

void debugRockCommand(client *c) {
    sds flag = c->argv[1]->ptr;
    // uint8_t dbid = c->db->id;

    if (strcasecmp(flag, "set") == 0) {
        robj *key = c->argv[2];
        serverLog(LL_WARNING, "key = %s", (sds)key->ptr);
        robj *val = lookupKeyRead(c->db, key);

        if (!val) {
            addReplyError(c, "can not find the key to set rock value");
            return;            
        }

        if (val->type != OBJ_STRING) {
            addReplyError(c, "key found, but type is not OBJ_STRING");
            return;
        }
        if (val == shared.keyRockVal) {
            addReplyError(c, "key found, but the value has already been shared.keyRockVal");
            return;
        }
        
        ++c->db->stat_key_str_rockval_cnt;
        // addRockWriteTaskOfString(dbid, key->ptr, old_str);
        // sdsfree(old_str);
    } else {
        addReplyError(c, "wrong flag for debugrock!");
        return;
    }

    addReplyBulk(c,c->argv[0]);
}

/* When command be executed, it will chck the arguments for all the keys which value is in RocksDB.
 * If value in RocksDB, it will set rockKeyNumber > 0 and add a task for another thread to read from RocksDB
 * Task will be done in async mode 
 * NOTE: the client c could by virtual client 
 * If the caller is for stream write, is_write_stream != 0 and we add task to both read_candidates and stream_wait_keys */
void checkAndSetRockKeyNumber(client *c, const int is_stream_write) {
    return;
    
    // Because stream write is seriable
    if (is_stream_write) serverAssert(dictSize(stream_wait_keys) == 0);

	serverAssert(c);
	serverAssert(c->streamWriting != STREAM_WRITE_WAITING);
	serverAssert(c->rockKeyNumber == 0);

    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);
    serverAssert(cmd);

    if (!cmd->rock_proc) return;

    list *rock_keys = cmd->rock_proc(c);
    if (!rock_keys) return;
    
    c->rockKeyNumber = listLength(rock_keys);
    serverAssert(c->rockKeyNumber > 0);

    lockRockRead();

    uint64_t client_id = c->id;
    listIter li;
    listNode *ln;
    listRewind(rock_keys, &li);
    while ((ln = listNext(&li))) {
        sds key = listNodeValue(ln);
        dictEntry *de = dictFind(read_candidates, key);
        if (!de) {
            list *new_clientids = listCreate();
            listAddNodeTail(new_clientids, (void *)client_id);
            dictAdd(read_candidates, key, new_clientids);
        } else {
            list *save_clientids = dictGetVal(de);
            listAddNodeTail(save_clientids, (void *)client_id);
        }
        if (is_stream_write)
            // NOTE: maybe add the same key serveral times
            dictAdd(stream_wait_keys, key, NULL);       
    }

    unlockRockRead();

    // clear list but not the keys. NOTE: keys are owned by read_candidates
    listRelease(rock_keys);
}

/* check that the command(s)(s for transaction) have any keys in rockdb 
 * if no keys in rocksdb, then resume the command
 * otherwise, client go on sleep for rocks */
void checkThenResumeRockClient(client *c) {
    serverAssert(c->rockKeyNumber == 0);
    int is_stream_write = (c->id == server.streamCurrentClientId);
    checkAndSetRockKeyNumber(c, is_stream_write);
    if (c->rockKeyNumber == 0) {
    	int res = processCommandAndResetClient(c);
	    serverLog(LL_WARNING, "resumeRockClient res = %d", res);
    }
}

/*                                 */
/*                                 */
/* The following is about write    */
/*                                 */
/*                                 */

/* this is the write thread entrance. Here we init rocksdb environment 
*  and then start a forever loop to write value to Rocksdb and deal with error */

void addRockWriteTaskOfString(uint8_t dbid, sds key, sds val) {
    sds rock_key, rock_val;
    struct WriteTask *task = zmalloc(sizeof(struct WriteTask));

    rock_key = sdsnewlen(&dbid, 1);
    rock_key = sdscatlen(rock_key, key, sdslen(key));
    rock_val = sdsdup(val);

    task->type = ROCK_STRING_TYPE;
    task->key = rock_key;
    task->val = rock_val;

    lockRockWrite();
    listAddNodeTail(write_queue, task);
    if (listLength(write_queue) > ROCK_WRITE_QUEUE_TOO_LONG)
        serverLog(LL_WARNING, "write queue of Rock is too long, length = %lu", listLength(write_queue));
    unlockRockWrite();
}

void addRockWriteTaskOfHash(uint8_t dbid, sds key, sds field, sds val) {
    sds rock_key, rock_val;
    struct WriteTask *task = zmalloc(sizeof(struct WriteTask));

    rock_key = sdsnewlen(&dbid, 1);
    uint32_t key_len = intrev32ifbe(sdslen(key));
    rock_key = sdscatlen(rock_key, &key_len, sizeof(key_len));
    rock_key = sdscatlen(rock_key, key, sdslen(key));
    rock_key = sdscatlen(rock_key, field, sdslen(field));
    rock_val = sdsdup(val);

    task->type = ROCK_HASH_TYPE;
    task->key = rock_key;
    task->val = rock_val;

    lockRockWrite();
    listAddNodeTail(write_queue, task);
    if (listLength(write_queue) >= ROCK_WRITE_QUEUE_TOO_LONG)
        serverLog(LL_WARNING, "write queue of Rock is too long, length = %lu", listLength(write_queue));
    unlockRockWrite();
}

/* We remove before_pick_cnt task which is regards as consumed in write_queue 
 * Then we pick some task up to the limit of ROCK_WRITE_PICK_MAX_LEN
 * and return the pick number  */   
static int pickWriteTasksInWriteThread(struct WriteTask **tasks, const int before_pick_cnt) {
    serverAssert(before_pick_cnt >= 0 && before_pick_cnt <= ROCK_WRITE_PICK_MAX_LEN);

    int cnt = 0;
    listIter li;
    listNode *ln;
    struct WriteTask* to_free_tasks[ROCK_WRITE_PICK_MAX_LEN];

    lockRockWrite();

    // first remove the consumed elements in write_queue
    if (before_pick_cnt) {
        serverAssert((int)listLength(write_queue) >= before_pick_cnt);
        listRewind(write_queue, &li);
        for (int i = 0; i < before_pick_cnt; ++i) {
            ln = listNext(&li);
            struct WriteTask *task = listNodeValue(ln);
            to_free_tasks[i] = task;
            listDelNode(write_queue, ln);
        }
    }

    listRewind(write_queue, &li);
    while ((ln = listNext(&li)) && cnt < ROCK_WRITE_PICK_MAX_LEN) {
        tasks[cnt] = listNodeValue(ln);
        ++cnt;        
    }

    unlockRockWrite();

    // release resource out of lock for performance reason
    for (int i = 0; i < before_pick_cnt; ++i) {
        struct WriteTask *to_free = to_free_tasks[i];
        sdsfree(to_free->key);
        sdsfree(to_free->val);
        zfree(to_free);
    }

    return cnt;
}

static void writeTasksToRocksdbInWriteThread(const int cnt, struct WriteTask **tasks) {
    rocksdb_writebatch_t* batch = rocksdb_writebatch_create();
    rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_disable_WAL(writeoptions, 1);
    char *err = NULL;

    for (int i = 0; i < cnt; ++i) {
        sds key = tasks[i]->key;
        sds val = tasks[i]->val;
        rocksdb_writebatch_put(batch, key, sdslen(key), val, sdslen(val));
    }
    rocksdb_write(rockdb, writeoptions, batch, &err);
    if (err) {
        serverLog(LL_WARNING, "writeTasksToRocksdbInWriteThread() failed reason = %s", err);
        exit(1);
    }

    rocksdb_writeoptions_destroy(writeoptions);
    rocksdb_writebatch_destroy(batch);
}

static void* entryInWriteThread(void *arg) {
    UNUSED(arg);

    uint sleepMicro = START_SLEEP_MICRO;
    struct WriteTask* tasks[ROCK_WRITE_PICK_MAX_LEN];
    int pick_cnt = 0;

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

void initRocksdb() {
    long cpus = sysconf(_SC_NPROCESSORS_ONLN);
    rocksdb_options_t *options = rocksdb_options_create();

    // Set # of online cores
    rocksdb_options_increase_parallelism(options, (int)(cpus));
    rocksdb_options_optimize_level_style_compaction(options, 0); 
    // create the DB if it's not already present
    rocksdb_options_set_create_if_missing(options, 1);

    // open DB
    char *err = NULL;
    rockdb = rocksdb_open(options, bunny_rockdb_path, &err);
    if (err) {
        serverLog(LL_WARNING, "initRocksdb() failed reason = %s", err);
        exit(1);
    }
        
    rocksdb_options_destroy(options);
}

void initRockWrite() {
    initRocksdb();

    pthread_t write_thread;

    write_queue = listCreate();

   int spin_init_res = pthread_spin_init(&writeLock, 0);
    if (spin_init_res != 0)
        serverPanic("Can not init rock write spin lock, error code = %d", spin_init_res);

    if (pthread_create(&write_thread, NULL, entryInWriteThread, NULL) != 0) 
        serverPanic("Unable to create a rock write thread.");
}

void closeRockdb() {
    rocksdb_close(rockdb);
}

const char* getRockdbPath() {
    return bunny_rockdb_path;
}

/*                                 */
/*                                 */
/* The following is about read     */
/*                                 */
/*                                 */

/* return C_ERR if on_fly_task_cnt has not been cleared by main thread, i.e., main thead has not consumed them */
/* we will copy the keys of task to pick_keys for thread safety or efficiecy */
static int pickReadTasksInReadThread(size_t *pick_cnt, sds *pick_keys) {
    size_t cnt = 0;

    lockRockRead();
    
    if (on_fly_task_cnt) {
        unlockRockRead();
        return C_ERR;
    }

    if (dictSize(read_candidates) == 0) {
        unlockRockRead();
        *pick_cnt = 0;
        return C_OK;
    }

    // we try to pick some read tasks because the stream_waiting_keys has higher priority
    dictIterator *di;
    dictEntry *de;

    int stream_cnt = dictSize(stream_wait_keys);
    if (stream_cnt) {
        di = dictGetIterator(stream_wait_keys);
        while ((de = dictNext(di))) {
            pick_keys[cnt] = dictGetVal(de);        // copy task key
            ++cnt;
            if (cnt == MAX_READ_TASK_NUM) break;
        }
        dictReleaseIterator(di);
    } else {
        di = dictGetIterator(read_candidates);
        while ((de = dictNext(di))) {
            pick_keys[cnt] = dictGetVal(de);
            ++cnt;
            if (cnt == MAX_READ_TASK_NUM) break;    // copy 
        }
    }

    on_fly_task_cnt = cnt;      // indicating there are some read tasks on fly

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

static void doReadTasksInReadThread(size_t task_cnt, sds *task_keys) {
    serverAssert(task_cnt > 0 && task_cnt <= MAX_READ_TASK_NUM);

    char* rockdb_vals[MAX_READ_TASK_NUM];
    size_t val_sizes[MAX_READ_TASK_NUM];
    char* errs[MAX_READ_TASK_NUM];

    size_t key_sizes[MAX_READ_TASK_NUM];
    for (size_t i = 0; i < task_cnt; ++i) {
        key_sizes[i] = sdslen(task_keys[i]);
    }

    rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
    rocksdb_multi_get(rockdb, readoptions, task_cnt, (const char* const *)task_keys, key_sizes, rockdb_vals, val_sizes, errs);
    rocksdb_readoptions_destroy(readoptions);

    // we need sds, not char*
    sds vals[MAX_READ_TASK_NUM];
    for (size_t i = 0; i < task_cnt; ++i) {
        if (errs[i]) {
            serverLog(LL_WARNING, "doReadTasksInReadThread() reading from RocksDB failed, err = %s, key = %s",
                errs[i], task_keys[i]);
            exit(1);
        }
        if (rockdb_vals[i] == NULL) {
            // not found
            vals[i] = val_not_found;
        } else {
            vals[i] = sdsnewlen(rockdb_vals[i], val_sizes[i]);
        }
        zlibc_free(rockdb_vals[i]);       // free the malloc() resource made by RocksDB API of rocksdb_multi_get()
    }

    lockRockRead();

    serverAssert(on_fly_task_cnt == task_cnt);
    for (size_t i = 0; i < task_cnt; ++i) {
        serverAssert(on_fly_vals[i] == NULL);
        on_fly_vals[i] = vals[i];
    }

    unlockRockRead();

    signalMainThreadByPipeInReadThread();
}

static void* entryInReadThread(void *arg) {
    UNUSED(arg);
    
    sds pick_keys[MAX_READ_TASK_NUM];
    size_t pick_cnt;

    uint sleepMicro = START_SLEEP_MICRO;
    while (1) {
        if (pickReadTasksInReadThread(&pick_cnt, pick_keys) == C_ERR) {
            // we sleep a little while for main thread to consume done_candidates
            usleep(START_SLEEP_MICRO);
            continue;
        }

        if (pick_cnt == 0) {
            usleep(sleepMicro);
            sleepMicro <<= 1;        // double sleep time
            if (sleepMicro > MAX_SLEEP_MICRO) 
                sleepMicro = MAX_SLEEP_MICRO;
        } else {
            doReadTasksInReadThread(pick_cnt, pick_keys);
        }
    }

    return NULL;
}

/* when read thread finish the read tasks, it sets the return values in on_fly_vals in doReadTasksInReadThread() 
 * and signal the main thread. 
 * Here is the signal handler of main thread */
static void rockReadSignalHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    UNUSED(fd);

    // clear pipe signal
    char tmpUseBuf[1];
    size_t n = read(server.rock_pipe_read, tmpUseBuf, 1);     
    serverAssert(n == 1);

    lockRockRead();
    size_t cnt = 0;
    for (size_t i = 0; i < MAX_READ_TASK_NUM; ++i) {
        if (on_fly_vals[i]) {
            ++cnt;
        } else {
            break;
        }        
    }

    for (size_t i = 0; i < cnt; ++i) {
        sds key = on_fly_keys[i];
        sds val = on_fly_vals[i];
        serverLog(LL_WARNING, "read key = %s, val = %s", key, val == val_not_found ? "Not Found!!!" : val);
    }
    unlockRockRead();
}

void initRockPipeAndRockRead() {
    pthread_t read_thread;
    int pipefds[2];
    read_candidates = dictCreate(&readCandidatesDictType, NULL);
    stream_wait_keys = dictCreate(&streamWaitKeyDictType, NULL);
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

