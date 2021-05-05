#include "server.h"

// please reference scripting.c
// we create a virtual client which will execute a command in the virtual client context

// reference scripting.c luaRedisGenericCommand
// right now for simplicity, we do not use cache
// from networking.c processInlineBuffer(), I guess all argument is string object
void execVritualCommand() {
    struct redisCommand *cmd;

    // for test
    uint8_t dbid = 0;
    sds cmdArg = sdsnew("set");
    sds keyArg = sdsnew("abc");
    sds valArg = sdsnew("123456");
    int argc = 3;

    client *c = server.virtual_client;
    serverAssert(dbid < server.dbnum);
    selectDb(c, dbid);
    c->argc = argc;
    c->argv = zmalloc(sizeof(robj*)*argc);
    c->argv[0] = createStringObject(cmdArg, sdslen(cmdArg));
    c->argv[1] = createStringObject(keyArg, sdslen(keyArg));
    c->argv[2] = createStringObject(valArg, sdslen(valArg));

    cmd = lookupCommand(c->argv[0]->ptr);
    serverAssert(cmd && !(cmd->flags & CMD_RANDOM));
    c->cmd = c->lastcmd = cmd;

    /* Check the ACLs. */
    int acl_errpos;
    int acl_retval = ACLCheckAllPerm(c,&acl_errpos);
    serverAssert(acl_retval == ACL_OK);

    /* Run the command */
    int call_flags = CMD_CALL_SLOWLOG | CMD_CALL_STATS;
    call(c,call_flags);
    serverAssert((c->flags & CLIENT_BLOCKED) == 0);

    // deal with reply. We do not need any reply info
    while(listLength(c->reply)) {
        listDelNode(c->reply,listFirst(c->reply));
    }
    c->bufpos = 0;
    c->reply_bytes = 0;

// cleanup:
    sdsfree(cmdArg);
    sdsfree(keyArg);
    sdsfree(valArg);

    for (int j = 0; j < c->argc; ++j) {
        robj* obj = c->argv[j];
        decrRefCount(obj);
    }

    zfree(c->argv);
    c->argc = 0;
    c->argv = NULL;
}