#ifndef __ROCKEVICT_H
#define __ROCKEVICT_H

#define ROCK_KEY_UPPER_PERCENTAGE 98

#define EVICT_ROCK_ENOUGH_MEM      1
#define EVICT_ROCK_NOT_READY       2
#define EVICT_ROCK_FREE            3
#define EVICT_ROCK_TIMEOUT         4

// API from server.c
int dictExpandAllowed(size_t moreMem, double usedRatio);

// API
unsigned long long estimateObjectIdleTimeFromLruDictEntry(dictEntry *de);
evictHash* lookupEvictOfHash(const uint8_t dbid, sds key);
int checkMemInProcessBuffer(client *c);
void debugEvictCommand(client *c);
int performeKeyOrHashEvictions(int must_do, size_t must_tofree, size_t *real_free);
void cronEvictToMakeRoom();
sds combine_dbid_key(const uint8_t dbid, const sds key);
void free_combine_dbid_key(sds to_free);
evictHash* removeHashCandidate(uint8_t dbid, sds key);
int checkAddToEvictHashCandidates(const uint8_t dbid, const size_t added, sds key);
void clearEvictByEmptyDb(uint8_t dbid);
void updatePureHashLru(uint8_t dbid, sds key, sds field);
void updatePureHashLruForWholeKey(uint8_t dbid, sds key);
void updateKeyLruForOneKey(uint8_t dbid, sds key);
// void refreshHashLru(uint8_t dbid, sds key, sds field);

// void debug_check_field_same(sds key, evictHash *evict_hash);

#endif