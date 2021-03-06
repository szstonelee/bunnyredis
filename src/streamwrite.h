
#ifndef __STREAMWRITE_H
#define __STREAMWRITE_H

// we need define two client id (uint64_t) which any valid client id can reach
#define NO_STREAM_CLIENT_ID UINT64_MAX
#define VIRTUAL_CLIENT_ID (UINT64_MAX - 1)

// when consumer is startingup and resume old messages form Kafka. It must be 255
#define CONSUMER_STARTUP_NODE_ID  255   

#define STREAM_CHECK_SET_STREAM           1     // set stream state
#define STREAM_CHECK_GO_ON_WITH_ERROR     2     // go on to execute command(including queued the command), check found some error
#define STREAM_CHECK_GO_ON_NO_ERROR       3     // go on to execute command(including queued the command), check does not found no error
#define STREAM_CHECK_ACL_FAIL             4     // failed by ACL check, just return
#define STREAM_CHECK_EMPTY_TRAN           5     // transaction is empty executution
#define STREAM_CHECK_FORBIDDEN            6     // forbidden by BunnyRedis
#define STREAM_CHECK_MSG_OVERFLOW         7     // the size of command with args is too large 

#define CONSUMER_STARTUP_START            0
#define CONSUMER_STARTUP_FINISH           1
#define CONSUMER_STARTUP_OPEN_TO_CLIENTS  2


// API from t_string.c
int parseExtendedStringArgumentsWithoutReply(client *c, int *flags, int *unit, robj **expire, int command_type);

// API from networking.c
void commandProcessed(client *c);

// API
void initStreamPipeAndStartConsumer();
void execVirtualCommand();
int checkAndSetStreamWriting(client *c);
void initKafkaProducer();
void setVirtualContextFromWillFreeConcreteClient(client *concrete);
void try_to_execute_stream_commands();

void lockConsumerData();
void unlockConsumerData();
extern redisAtomic int kafkaStartupConsumeFinish;

int is_startup_on_going();

#endif