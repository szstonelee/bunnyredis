#include "server.h"
#include "rock.h"
#include "networking.h"

void _debugClient(client *c) {
	int argc = c->argc;
	serverLog(LL_WARNING, "debug client: argc = %d", argc);
	if (argc > 0) {
		for (int i = 0; i < argc; ++i) {
			serverLog(LL_WARNING, "debug client: argv[%d], %s", i, (char*)c->argv[i]->ptr);
		}
	}
}

/* caller guarentee the client not wait for rock keys
 * usually called by checkThenResumeRockClient() */
void _resumeRockClient(client *c) {
	serverAssert(c->rockKeyNumber == 0);
	_debugClient(c);
	int res = processCommandAndResetClient(c);
	serverLog(LL_WARNING, "resumeRockClient res = %d", res);
	return;	
}

/* When command be executed, it will chck the arguments for all the keys which value is in RocksDB.
 * If value in RocksDB, it will set rockKeyNumber > 0 and add a task for another thread to read from RocksDB
 * Task will be done in async mode */
void checkCallValueInRock(client *c) {
	serverAssert(c);
	serverAssert(c->streamWriting != STREAM_WRITE_WAITING);
	serverAssert(c->rockKeyNumber == 0);

	// for test
	/*
	char *cmd = c->argv[0]->ptr;
	char *key = c->argv[1]->ptr;
	if (strcmp(cmd, "get") == 0 && strcmp(key, "abc") == 0) {
		serverLog(LL_WARNING, "Suspend cmd = get and key == abc");
		c->rockKeyNumber = 1;
		c_abc = c;
	} else if (strcmp(key, "unlock") == 0) {
		serverLog(LL_WARNING, "unlcok");
		c_abc->rockKeyNumber = 0;
	}
	*/
}

/* check that the command(s)(s for transaction) have any keys in rockdb 
 * if no keys in rocksdb, then resume the command
 * otherwise, client go on sleep for rocks */
void checkThenResumeRockClient(client *c) {
    serverAssert(c->rockKeyNumber == 0);
    checkCallValueInRock(c);
    if (c->rockKeyNumber == 0) 
        _resumeRockClient(c);
}
