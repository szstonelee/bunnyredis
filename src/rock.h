#ifndef __ROCK_H
#define __ROCK_H

#include "server.h"

/* API */
void checkCallValueInRock(client *c);
void processInputBuffer(client *c);   // from networking.c

#endif