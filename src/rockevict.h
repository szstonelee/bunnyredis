#ifndef __ROCKEVICT_H
#define __ROCKEVICT_H

#define ROCK_KEY_UPPER_PERCENTAGE 98

#define EVICT_ROCK_OK         0
#define EVICT_ROCK_PERCENTAGE 1
#define EVICT_ROCK_TIMEOUT    2

int checkMemInProcessBuffer(client *c);
void debugEvictCommand(client *c);
int performKeyOfStringEvictions();

#endif