#include "server.h"
#include <librdkafka/rdkafka.h>

// spin lock only run in Linux
static pthread_spinlock_t consumerLock;     
static pthread_spinlock_t producerLock;

#define START_SLEEP_MICRO   16
#define MAX_SLEEP_MICRO     1024            // max sleep for 1 ms

list* sndMsgs;

long long startTime;
long long endTime;

static void lockConsumerData() {
    int res = pthread_spin_lock(&consumerLock);
    serverAssert(res == 0);
}

static void unlockConsumerData() {
    int res = pthread_spin_unlock(&consumerLock);
    serverAssert(res == 0);
}

static void lockProducerData() {
    int res = pthread_spin_lock(&producerLock);
    serverAssert(res == 0);
}

static void unlockProducerData() {
    int res = pthread_spin_unlock(&producerLock);
    serverAssert(res == 0);
}

static void signalByPipeInConsumerThread() {
    /* signal main thread rockPipeReadHandler()*/
    char tempBuf[1] = "a";
    size_t n = write(server.stream_pipe_write, tempBuf, 1);
    serverAssert(n == 1);
}

static void recvSignalByPipe() {
    char tmpUseBuf[1];
    size_t n = read(server.stream_pipe_read, tmpUseBuf, 1);     
    serverAssert(n == 1);
}

/* the event handler is executed from main thread, which is signaled by the pipe
 * from the rockdb thread. When it is called by the eventloop, there is 
 * a return result in rockJob */
static void _streamConsumerSignalHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    UNUSED(fd);

    recvSignalByPipe();

    lockConsumerData();
    endTime = ustime();
    unlockConsumerData();
    // serverLog(LL_WARNING, "latency = %lld", endTime - startTime);
}

/* this is the consumer thread entrance */
static void* _entryInConsumerThread(void *arg) {
    UNUSED(arg);
    uint sleepMicro = 1024;      // test for 1 ms

    while(1) {
        usleep(sleepMicro);
        lockConsumerData();
        startTime = ustime();
        unlockConsumerData();
        
        signalByPipeInConsumerThread();
    }

    return NULL;
}

/* the function create a stream reader thread, which will read data from kafka 
 * when stream reader thread read some write operations from kafka,
 * it will write it to buffer (sync by spin lock) then notify main thread by pipe 
 * because main thread maybe sleep in eventloop for events comming */
void initStreamPipeAndStartConsumer() {
    pthread_t consumer_thread;
    int pipefds[2];

    int spin_init_res = pthread_spin_init(&consumerLock, 0);
    if (spin_init_res != 0)
        serverPanic("Can not init consumer spin lock, error code = %d", spin_init_res);

    if (pipe(pipefds) == -1) 
        serverPanic("Can not create pipe for stream.");

    server.stream_pipe_read = pipefds[0];
    server.stream_pipe_write = pipefds[1];

    if (aeCreateFileEvent(server.el, server.stream_pipe_read, 
        AE_READABLE, _streamConsumerSignalHandler,NULL) == AE_ERR) {
        serverPanic("Unrecoverable error creating server.rock_pipe file event.");
    }

    if (pthread_create(&consumer_thread, NULL, _entryInConsumerThread, NULL) != 0) {
        serverPanic("Unable to create a consumer thread.");
    }
}

// please reference scripting.c
// we create a virtual client which will execute a command in the virtual client context

// reference server.c processCommand()
// return C_ERR if the command check failed for some reason
int checkAndSetStreamWriting(client *c) {
    serverAssert(c->streamWriting == STREAM_WRITE_INIT);

    // quit command is a shortcut
    if (!strcasecmp(c->argv[0]->ptr,"quit")) return C_OK;

    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);

    // command name can not parse 
    if (!cmd) return C_ERR;

    if (cmd->streamCmdCategory == STREAM_FORBIDDEN_CMD)
        return C_ERR;

    // command basic parameters number is not OK
    if ((cmd->arity > 0 && cmd->arity != c->argc) ||
        (c->argc < -cmd->arity)) 
        return C_ERR;
 
    // NOTE: we can not directly use processCommand() code 
    // because c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr); in processCommand()
    // here we does not set c->cmd and c->lastcmd

    /* Check if the user is authenticated. This check is skipped in case
     * the default user is flagged as "nopass" and is active. */
    int auth_required = (!(DefaultUser->flags & USER_FLAG_NOPASS) ||
                          (DefaultUser->flags & USER_FLAG_DISABLED)) &&
                        !c->authenticated;
    if (auth_required) {
        /* AUTH and HELLO and no auth modules are valid even in
         * non-authenticated state. */
        if (!(cmd->flags & CMD_NO_AUTH)) return C_ERR;
    }

    // check ACL, NOTE: we need to recover the c's cmd and lastcmd
    struct redisCommand *savedCmd = c->cmd;
    struct redisCommand *savedLastcmd = c->lastcmd;

    c->cmd = c->lastcmd = cmd;

    int acl_errpos;
    int acl_retval = ACLCheckAllPerm(c,&acl_errpos);
    if (acl_retval != ACL_OK) {
        c->cmd = savedCmd;
        c->lastcmd = savedLastcmd;
        return C_ERR;
    }

    /* Only allow a subset of commands in the context of Pub/Sub if the
     * connection is in RESP2 mode. With RESP3 there are no limits. */
    if ((c->flags & CLIENT_PUBSUB && c->resp == 2) &&
        c->cmd->proc != pingCommand &&
        c->cmd->proc != subscribeCommand &&
        c->cmd->proc != unsubscribeCommand &&
        c->cmd->proc != psubscribeCommand &&
        c->cmd->proc != punsubscribeCommand &&
        c->cmd->proc != resetCommand) {
        c->cmd = savedCmd;
        c->lastcmd = savedLastcmd;
        return C_ERR;        
    }

    if (cmd->streamCmdCategory == STREAM_ENABLED_CMD)
        c->streamWriting = STREAM_WRITE_WAITING;

    // recover client to saved info
    c->cmd = savedCmd;
    c->lastcmd = savedLastcmd;
    return C_OK;
}

// reference scripting.c luaRedisGenericCommand
// right now for simplicity, we do not use cache
// from networking.c processInlineBuffer(), I guess all argument is string object
void execVritualCommand() {
    struct redisCommand *cmd;

    // for test
    uint8_t dbid = 0;
    sds cmdArg = sdsnew("set");
    sds keyArg = sdsnew("abc");
    sds valArg = sdsnew("123456");
    int argc = 3;

    client *c = server.virtual_client;
    serverAssert(dbid < server.dbnum);
    selectDb(c, dbid);
    c->argc = argc;
    c->argv = zmalloc(sizeof(robj*)*argc);
    c->argv[0] = createStringObject(cmdArg, sdslen(cmdArg));
    c->argv[1] = createStringObject(keyArg, sdslen(keyArg));
    c->argv[2] = createStringObject(valArg, sdslen(valArg));

    cmd = lookupCommand(c->argv[0]->ptr);
    serverAssert(cmd && !(cmd->flags & CMD_RANDOM));
    c->cmd = c->lastcmd = cmd;

    /* Check the ACLs. */
    int acl_errpos;
    int acl_retval = ACLCheckAllPerm(c,&acl_errpos);
    serverAssert(acl_retval == ACL_OK);

    /* Run the command */
    int call_flags = CMD_CALL_SLOWLOG | CMD_CALL_STATS;
    call(c,call_flags);
    serverAssert((c->flags & CLIENT_BLOCKED) == 0);

    // deal with reply. We do not need any reply info
    while(listLength(c->reply)) {
        listDelNode(c->reply,listFirst(c->reply));
    }
    c->bufpos = 0;
    c->reply_bytes = 0;

// cleanup:
    sdsfree(cmdArg);
    sdsfree(keyArg);
    sdsfree(valArg);

    for (int j = 0; j < c->argc; ++j) {
        robj* obj = c->argv[j];
        decrRefCount(obj);
    }

    zfree(c->argv);
    c->argc = 0;
    c->argv = NULL;
}

/*                                 */
/*                                 */
/* The following is about producer */
/*                                 */
/*                                 */

/* this is be called in produer thread */
static void dr_msg_cb(rd_kafka_t *rk,
                      const rd_kafka_message_t *rkmessage, void *opaque) {
    UNUSED(rk);

    serverAssert(opaque);
    
    if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR) 
        // permanent error
        serverPanic("%% Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
    
    if (!rkmessage->err)
        sdsfree((sds)opaque);   // when success delivered, we free the msg
}

static void error_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
    UNUSED(rk);
    UNUSED(opaque);

    if (err != RD_KAFKA_RESP_ERR__FATAL)
        serverPanic("%% Error: %s: %s\n", rd_kafka_err2name(err), reason);
}

static sds pickSndMsg() {
    listIter li;
    listNode *ln;
    sds msg;

    if (listLength(sndMsgs) == 0) return NULL;

    lockProducerData();
    listRewind(sndMsgs, &li);
    ln = listNext(&li);
    serverAssert(ln);
    msg = listNodeValue(ln);
    listDelNode(sndMsgs, ln);
    unlockProducerData();

    return msg;
}

static void sendKafkaMsgInProducerThread(sds msg, rd_kafka_t *rk, rd_kafka_topic_t *rkt) {
    rd_kafka_resp_err_t err;

    serverAssert(msg);

    int retryTotalMs = 0;
    int retryMs = 16;
    while (retryTotalMs < 10000) {   // retry max time is 10 seconds
        // we must specify the partion zero for order msg
        // seet msgflags to 0, we does not need copy or free the payload
        // the payload will be freed in call back function
        err = rd_kafka_produce(rkt, 0, 0, msg, sdslen(msg), NULL, 0, msg);

        if (!err) return;

        if (err != RD_KAFKA_RESP_ERR__QUEUE_FULL) break;  // fatel error
        
        rd_kafka_poll(rk, retryMs);    // block for retry
        retryTotalMs += retryMs;
        retryMs <<= 1;  // double retry time
    }
    
    serverPanic("sendKafkaMsgInProducerThread() failed for tring %d times", retryTotalMs);
}

/* this is the producer thread entrance. Here we init kafka prodcer environment 
*  and then start a forever loop for send messages and dealt with error */
static void* _entryInProducerThread(void *arg) {
    UNUSED(arg);

    sndMsgs = listCreate();
    uint sleepMicro = START_SLEEP_MICRO;      

    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt; 
    char *brokers = "127.0.0.1:9092,";
    char *topic = "redisStreamWrite";
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topicConf;
    char errstr[512];  

    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer() failed for rd_kafka_conf_set() bootstrap.servers, reason = %s", errstr);
    // set time out to be infinite
    if (rd_kafka_conf_set(conf, "message.timeout.ms", "0",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer() failed for rd_kafka_conf_set() message.timeout.ms, reason = %s", errstr);
    if (rd_kafka_conf_set(conf, "enable.idempotence", "true",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer() failed for rd_kafka_conf_set() enable.idempotence, reason = %s", errstr);
    // set broker message size (to 100 M)
    if (rd_kafka_conf_set(conf, "message.max.bytes", "100000000",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer() failed for rd_kafka_conf_set() message.max.bytes, reason = %s", errstr);
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
    rd_kafka_conf_set_error_cb(conf, error_cb);
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk)
        serverPanic("initKafkaProducer() failed for rd_kafka_new(), reason = %s", errstr);
    topicConf = rd_kafka_topic_conf_new();
    if (!topicConf)
        serverPanic("initKafkaProducer() failed for rd_kafka_topic_conf_new()");
    rkt = rd_kafka_topic_new(rk, topic, topicConf);
    if (!rkt)
        serverPanic("initKafkaProducer() failed for rd_kafka_topic_new(), errno = %d,", errno);

    while(1) {
        sds msg;
        if ((msg = pickSndMsg())) {
            sendKafkaMsgInProducerThread(msg, rk, rkt);
            sleepMicro = START_SLEEP_MICRO;
            continue;
        }

        usleep(sleepMicro);
        sleepMicro <<= 1;        // double sleep time
        if (sleepMicro > MAX_SLEEP_MICRO) 
            sleepMicro = MAX_SLEEP_MICRO;
    }

    return NULL;
}

/* we use only one thread for producer, rdkafka library use multithread internally.
 * but main thead need use spin lock to guarantee no context switch to kernel  
 * for kafka idompootent C code, reference https://github.com/edenhill/librdkafka/blob/master/examples/idempotent_producer.c */
void initKafkaProducer() {
    pthread_t producer_thread;

   int spin_init_res = pthread_spin_init(&producerLock, 0);
    if (spin_init_res != 0)
        serverPanic("Can not init producer spin lock, error code = %d", spin_init_res);

    if (pthread_create(&producer_thread, NULL, _entryInProducerThread, NULL) != 0) 
        serverPanic("Unable to create a producer thread.");
}

