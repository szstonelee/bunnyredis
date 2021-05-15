#ifndef __ROCK_H
#define __ROCK_H

#include "server.h"

/* API */
void checkAndSetRockKeyNumber(client *c, const int is_stream_write);
void processInputBuffer(client *c);   // from networking.c
void addRockWriteTaskOfString(uint8_t dbid, sds key, sds val);
void addRockWriteTaskOfHash(uint8_t dbid, sds key, sds field, sds val);
void initRockWrite();
void initRockPipeAndRockRead();
void closeRockdb();
const char* getRockdbPath();
void debugRockCommand(client *c);

list* getCmdForRock();

#endif