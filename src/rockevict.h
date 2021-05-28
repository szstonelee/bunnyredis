#ifndef __ROCKEVICT_H
#define __ROCKEVICT_H

#define ROCK_KEY_UPPER_PERCENTAGE 98

#define EVICT_ROCK_ENOUGH_MEM      1
#define EVICT_ROCK_NOT_READY       2
#define EVICT_ROCK_FREE            3
#define EVICT_ROCK_TIMEOUT         4

/* stored in dict of server.evict_hash_candidates as value */
typedef struct evictHash {
    dict* field_lru;
    long long rock_cnt;
    sds key;
    dict *dict_hash;     // point to the hash dictionary
} evictHash;

// API from server.c
int dictExpandAllowed(size_t moreMem, double usedRatio);

// API
evictHash* lookupEvictOfHash(const uint8_t dbid, sds key);
int checkMemInProcessBuffer(client *c);
void debugEvictCommand(client *c);
int performeKeyOfStringOrHashEvictions(int must_do, size_t must_tofree);
void cronEvictToMakeRoom();
sds combine_dbid_key(const uint8_t dbid, const sds key);
void free_combine_dbid_key(sds to_free);
int removeHashCandidate(sds combined_key);
int checkAddToEvictHashCandidates(const uint8_t dbid, const size_t added, sds key, dict *dict);
void clearEvictByEmptyDb(uint8_t dbid);
void updateHashLru(uint8_t dbid, sds key, sds field);
void updateHashLruForKey(uint8_t dbid, sds key);
void setForHashLru(uint8_t dbid, sds key, sds field);

#endif