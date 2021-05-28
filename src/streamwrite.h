
#ifndef __STREAMWRITE_H
#define __STREAMWRITE_H

// we need define two client id (uint64_t) which any valid client id can reach
#define NO_STREAM_CLIENT_ID UINT64_MAX
#define VIRTUAL_CLIENT_ID (UINT64_MAX - 1)

#define STREAM_CHECK_SET_STREAM           1     // set stream state
#define STREAM_CHECK_GO_ON_WITH_ERROR     2     // go on to execute command(including queued the command), check found some error
#define STREAM_CHECK_GO_ON_NO_ERROR       3     // go on to execute command(including queued the command), check does not found no error
#define STREAM_CHECK_ACL_FAIL             4     // failed by ACL check, just return
#define STREAM_CHECK_EMPTY_TRAN           5     // transaction is empty executution
#define STREAM_CHECK_FORBIDDEN            6     // forbidden by BunnyRedis

// API from t_string.c
int parseExtendedStringArgumentsOrReply(client *c, int *flags, int *unit, robj **expire, int command_type, int from_strem_check);

// API
void initStreamPipeAndStartConsumer();
void execVirtualCommand();
int checkAndSetStreamWriting(client *c);
void initKafkaProducer();
void setVirtualContextFromConcreteClient(client *concrete);
void try_to_execute_stream_commands();

#endif