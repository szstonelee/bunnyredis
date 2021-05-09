#ifndef __ROCK_H
#define __ROCK_H

#include "server.h"

/* API */
void checkAndSetRockKeyNumber(client *c);
void processInputBuffer(client *c);   // from networking.c

#endif