
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

dict *stream_wait_keys;
dict *read_candidates;

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

/* When command be executed, it will chck the arguments for all the keys which value is in RocksDB.
 * If value in RocksDB, it will set rockKeyNumber > 0 and add a task for another thread to read from RocksDB
 * Task will be done in async mode 
 * NOTE: the client c could by virtual client 
 * If the caller is for stream write, is_write_stream != 0 */
void checkAndSetRockKeyNumber(client *c, const int is_stream_write) {
    UNUSED(is_stream_write);

	serverAssert(c);
	serverAssert(c->streamWriting != STREAM_WRITE_WAITING);
	serverAssert(c->rockKeyNumber == 0);

    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);
    serverAssert(cmd);
    list *rock_keys = NULL;
    if (cmd->rock_proc) {
        rock_keys = cmd->rock_proc(c);
    }

/*
    if (c->argc > 1 && 
        strcasecmp("set", c->argv[0]->ptr) == 0 &&
        strcasecmp("abc", c->argv[1]->ptr) == 0)
        c->rockKeyNumber = 1;       // for test
*/


	// for test
	/*
	char *cmd = c->argv[0]->ptr;
	char *key = c->argv[1]->ptr;
	if (strcmp(cmd, "get") == 0 && strcmp(key, "abc") == 0) {
		serverLog(LL_WARNING, "Suspend cmd = get and key == abc");
		c->rockKeyNumber = 1;
		c_abc = c;
	} else if (strcmp(key, "unlock") == 0) {
		serverLog(LL_WARNING, "unlcok");
		c_abc->rockKeyNumber = 0;
	}
	*/
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
        serverLog(LL_NOTICE, "write to Rocksdb, key = %s", key+1);
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


static void* entryInReadThread(void *arg) {
    UNUSED(arg);
    return NULL;
}

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
    unlockRockRead();
}

void initRockPipeAndRockRead() {
    pthread_t read_thread;
    int pipefds[2];
    read_candidates = dictCreate(&readCandidatesDictType, NULL);
    stream_wait_keys = dictCreate(&streamWaitKeyDictType, NULL);

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

