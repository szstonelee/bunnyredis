
#ifndef __STREAMWRITE_H
#define __STREAMWRITE_H

// we need define two client id (uint64_t) which any valid client id can reach
#define NO_STREAM_CLIENT_ID UINT64_MAX
#define VIRTUAL_CLIENT_ID (UINT64_MAX - 1)

void initStreamPipeAndStartConsumer();
void execVirtualCommand();
int checkAndSetStreamWriting(client *c);
void initKafkaProducer();
void setVirtualContextFromConcreteClient(client *concrete);

#endif