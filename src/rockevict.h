#ifndef __ROCKEVICT_H
#define __ROCKEVICT_H

int checkMemInProcessBuffer(client *c);
void debugEvictCommand(client *c);
int performKeyOfStringEvictions();

#endif