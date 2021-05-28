#ifndef __ROCK_H
#define __ROCK_H

#include "server.h"

// from networking.c, because Redis does not provide networking.h
void processInputBuffer(client *c);
int processCommandAndResetClient(client *c);

// from t_string.c
list* stringGenericGetOneKeyForRock(client *c);
list* stringGenericGetMultiKeysForRock(client *c, int start_index, int step);

// API support t_string.c
list* genericGetOneKeyForRock(client *c, int index);

/* API */
void checkAndSetRockKeyNumber(client *c, const int is_stream_write);
void addRockWriteTaskOfString(uint8_t dbid, sds key, sds val);
void addRockWriteTaskOfHash(uint8_t dbid, sds key, sds field, sds val);
void initRockWrite();
void initRockPipeAndRockRead();
void closeRockdb();
const char* getRockdbPath();
void debugRockCommand(client *c);
void update_rock_stat_and_try_delete_evict_candidate_for_db_delete(redisDb *db, dictEntry* de);

sds encode_rock_key_for_string(const uint8_t dbid, sds const string_key);
sds encode_rock_key_for_hash(const uint8_t dbid, sds const key, sds const field);

/* Command check rock value API */
// keys
list* copyCmdForRock(client *c);
list* dumpCmdForRock(client *c);
list* moveCmdForRock(client *c);
list* objectCmdForRock(client *c);
list* renameCmdForRock(client *c);
list* renamenxCmdForRock(client *c);
// string
list* getCmdForRock(client *c);
list* appendCmdForRock(client *c);
list* bitcountCmdForRock(client *c);
list* bitfieldCmdForRock(client *c);
list* bitopCmdForRock(client *c);
list* bitposCmdForRock(client *c);
list* decrCmdForRock(client *c);
list* decrbyCmdForRock(client *c);
list* getbitCmdForRock(client *c);
list* getdelCmdForRock(client *c);
list* getrangeCmdForRock(client *c);
list* getsetCmdForRock(client *c);
list* incrCmdForRock(client *c);
list* incrbyCmdForRock(client *c);
list* incrbyfloatCmdForRock(client *c);
list* mgetCmdForRock(client *c);
list* msetCmdForRock(client *c);
list* msetnxCmdForRock(client *c);
list* setbitCmdForRock(client *c);
list* setnxCmdForRock(client *c);
list* setrangeCmdForRock(client *c);
list* strlenCmdForRock(client *c);
// transaction
list* execCmdForRock(client *c);
// hash
list* hgetCmdForRock(client *c);
list* hgetallCmdForRock(client *c);
list* hincrbyCmdForRock(client *c);
list* hincrbyfloatCmdForRock(client *c);
list* hmgetCmdForRock(client *c);
list* hstrlenCmdForRock(client *c);
list* hvalsCmdForRock(client *c);

#endif