#ifndef __ROCK_H
#define __ROCK_H

#include "server.h"

// from networking.c, because Redis does not provide networking.h
void processInputBuffer(client *c);
int processCommandAndResetClient(client *c);

// from t_string.c
// list* stringGenericGetOneKeyForRock(client *c);
list* stringGenericGetMultiKeysForRock(client *c, int start_index, int step);

// API support from t_string.c
// list* genericGetOneKeyForRock(client *c, int index);


/* API */
size_t get_rock_write_queue_len();
void checkAndSetRockKeyNumber(client *c, const int is_stream_write);
void initRockWrite();
void initRockPipeAndRockRead();
void closeRockdb();
const char* getRockdbPath();
void debugRockCommand(client *c);
void update_rock_stat_and_try_delete_evict_candidate_for_db_delete(redisDb *db, dictEntry* de);
int debug_set_string_key_rock(uint8_t dbid, sds key);

void addRockWriteTaskOfString(uint8_t dbid, sds key, sds val);
void addRockWriteTaskOfZiplist(uint8_t dbid, sds key, unsigned char *zl);
void addRockWriteTaskOfHash(uint8_t dbid, sds key, sds field, sds val);
sds encode_rock_key_for_string(const uint8_t dbid, sds const string_key);
sds encode_rock_key_for_ziplist(const uint8_t dbid, sds const string_key);
sds encode_rock_key_for_hash(const uint8_t dbid, sds const key, sds const field);

/* API for rock to support the following API */
list* hGenericGetOneKeyOfZiplistForRock(client *c);
list* hGenericGetAllFieldForRock(client *c);
list* genericGetOneKeyExcludePureHashForRock(client *c, int index);
list* genericGetOneKeyForRock(client *c);
list* hGenericGetOneFieldForRock(client *c);
list* hGenericRockForZiplist(uint8_t dbid, sds key, robj *o);

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
list* hsetCmdForRock(client *c);
list* hsetnxCmdForRock(client *c);
list* hexistsCmdForRock(client *c);
list* hkeysCmdForRock(client *c);
list* hlenCmdForRock(client *c);
list* hmsetCmdForRock(client *c);
list* hrandfieldCmdForRock(client *c);
list* hdelCmdForRock(client *c);
// hyperloglog
list* pfaddCmdForRock(client *c);
list* pfcountCmdForRock(client *c);
list* pfmergeCmdForRock(client *c);

#endif