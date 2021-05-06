
#ifndef __STREAMWRITE_H
#define __STREAMWRITE_H

void initStreamPipeAndStartConsumer();
void execVritualCommand();
int checkAndSetStreamWriting(client *c);
void initKafkaProducer();

#endif