#include "server.h"

// spin lock only run in Linux
static pthread_spinlock_t consumerLock;     
static pthread_spinlock_t producerLock;


long long startTime;
long long endTime;

void lockConsumerData() {
    int res = pthread_spin_lock(&consumerLock);
    serverAssert(res == 0);
}

void unlockConsumerData() {
    int res = pthread_spin_unlock(&consumerLock);
    serverAssert(res == 0);
}

void lockProducerData() {
    int res = pthread_spin_lock(&producerLock);
    serverAssert(res == 0);
}

void unlockProducerData() {
    int res = pthread_spin_unlock(&producerLock);
    serverAssert(res == 0);
}

void signalByPipeInConsumerThread() {
    /* signal main thread rockPipeReadHandler()*/
    char tempBuf[1] = "a";
    size_t n = write(server.stream_pipe_write, tempBuf, 1);
    serverAssert(n == 1);
}

void recvSignalByPipe() {
    char tmpUseBuf[1];
    size_t n = read(server.stream_pipe_read, tmpUseBuf, 1);     
    serverAssert(n == 1);
}

/* the event handler is executed from main thread, which is signaled by the pipe
 * from the rockdb thread. When it is called by the eventloop, there is 
 * a return result in rockJob */
void _streamConsumerSignalHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
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
void* _entryInConsumerThread(void *arg) {
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


/* this is the producer thread entrance */
void* _entryInProducerThread(void *arg) {
    UNUSED(arg);
    uint sleepMicro = 1024;      // test for 1 second

    while(1) {
        usleep(sleepMicro);
        lockProducerData();
        unlockProducerData();
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

void startStreamProducer() {
    pthread_t producer_thread;

   int spin_init_res = pthread_spin_init(&producerLock, 0);
    if (spin_init_res != 0)
        serverPanic("Can not init producer spin lock, error code = %d", spin_init_res);

    if (pthread_create(&producer_thread, NULL, _entryInProducerThread, NULL) != 0) {
        serverPanic("Unable to create a producer thread.");
    }
}
