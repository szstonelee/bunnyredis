#include "server.h"

#include "virtual.h"

// please reference scripting.c
// we create a virtual client which will execute a command in the virtual client context

// reference server.c processCommand()
// return C_ERR if the command check failed for some reason
int checkAndSetStreamWriting(client *c) {
    serverAssert(c->streamWriting == STREAM_WRITE_INIT);

    // quit command is a shortcut
    if (!strcasecmp(c->argv[0]->ptr,"quit")) return C_OK;

    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);

    // command name can not parse 
    if (!cmd) return C_ERR;

    if (cmd->streamCmdCategory == STREAM_FORBIDDEN_CMD)
        return C_ERR;

    // command basic parameters number is not OK
    if ((cmd->arity > 0 && cmd->arity != c->argc) ||
        (c->argc < -cmd->arity)) 
        return C_ERR;
 
    // NOTE: we can not directly use processCommand() code 
    // because c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr); in processCommand()
    // here we does not set c->cmd and c->lastcmd

    /* Check if the user is authenticated. This check is skipped in case
     * the default user is flagged as "nopass" and is active. */
    int auth_required = (!(DefaultUser->flags & USER_FLAG_NOPASS) ||
                          (DefaultUser->flags & USER_FLAG_DISABLED)) &&
                        !c->authenticated;
    if (auth_required) {
        /* AUTH and HELLO and no auth modules are valid even in
         * non-authenticated state. */
        if (!(cmd->flags & CMD_NO_AUTH)) return C_ERR;
    }

    // check ACL, NOTE: we need to recover the c's cmd and lastcmd
    struct redisCommand *savedCmd = c->cmd;
    struct redisCommand *savedLastcmd = c->lastcmd;

    c->cmd = c->lastcmd = cmd;

    int acl_errpos;
    int acl_retval = ACLCheckAllPerm(c,&acl_errpos);
    if (acl_retval != ACL_OK) {
        c->cmd = savedCmd;
        c->lastcmd = savedLastcmd;
        return C_ERR;
    }

    /* Only allow a subset of commands in the context of Pub/Sub if the
     * connection is in RESP2 mode. With RESP3 there are no limits. */
    if ((c->flags & CLIENT_PUBSUB && c->resp == 2) &&
        c->cmd->proc != pingCommand &&
        c->cmd->proc != subscribeCommand &&
        c->cmd->proc != unsubscribeCommand &&
        c->cmd->proc != psubscribeCommand &&
        c->cmd->proc != punsubscribeCommand &&
        c->cmd->proc != resetCommand) {
        c->cmd = savedCmd;
        c->lastcmd = savedLastcmd;
        return C_ERR;        
    }

    if (cmd->streamCmdCategory == STREAM_ENABLED_CMD)
        c->streamWriting = STREAM_WRITE_WAITING;

    // recover client to saved info
    c->cmd = savedCmd;
    c->lastcmd = savedLastcmd;
    return C_OK;
}

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