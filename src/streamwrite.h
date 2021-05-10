
#ifndef __STREAMWRITE_H
#define __STREAMWRITE_H

void initStreamPipeAndStartConsumer();
void execVirtualCommand();
int checkAndSetStreamWriting(client *c);
void initKafkaProducer();
void setVirtualContextFromConcreteClient(client *concrete);

#endif