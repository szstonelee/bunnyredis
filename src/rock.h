#ifndef __ROCK_H
#define __ROCK_H

#include "server.h"

// from networking.c, because Redis does not provide networking.h
void processInputBuffer(client *c);
int processCommandAndResetClient(client *c);

/* API */
void checkAndSetRockKeyNumber(client *c, const int is_stream_write);
void addRockWriteTaskOfString(uint8_t dbid, sds key, sds val);
void addRockWriteTaskOfHash(uint8_t dbid, sds key, sds field, sds val);
void initRockWrite();
void initRockPipeAndRockRead();
void closeRockdb();
const char* getRockdbPath();
void debugRockCommand(client *c);

sds encode_rock_key_for_string(const uint8_t dbid, sds const string_key);
sds encode_rock_key_for_hash(const uint8_t dbid, sds const key, sds const field);

/* Command check rock value API */
list* getCmdForRock(client *c);
list* appendCmdForRock(client *c);
list* execCmdForRock(client *c);
list* hgetCmdForRock(client *c);

#endif