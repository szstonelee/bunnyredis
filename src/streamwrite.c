
#include "server.h"

#include "endianconv.h"
#include "rock.h"
#include "streamwrite.h"

#include <librdkafka/rdkafka.h>
#include <uuid/uuid.h>


// spin lock only run in Linux
static pthread_spinlock_t consumerLock;     
static pthread_spinlock_t producerLock;

#define START_SLEEP_MICRO   16
#define MAX_SLEEP_MICRO     1024    // max sleep for 1 ms

#define START_OFFSET        0       // consumer start offset to read

#define RCV_MSGS_TOO_LONG   1000    // if we recevice to much messages and not quick to consume, it just warns

static int kafkaReadyAndTopicCorrect = 0;   // producer start for check kafka state and main thread check it for correctness
static const char *bootstrapBrokers = "127.0.0.1:9092,";
static const char *bunnyRedisTopic = "redisStreamWrite"; 

static list* sndMsgs;  // producer and main thread contented data
static list* rcvMsgs;  // consumer and main thread contented data

static sds sds_exec;    // global variable which is "exec" string

static void lockConsumerData() {
    int res = pthread_spin_lock(&consumerLock);
    serverAssert(res == 0);
}

static void unlockConsumerData() {
    int res = pthread_spin_unlock(&consumerLock);
    serverAssert(res == 0);
}

static void lockProducerData() {
    int res = pthread_spin_lock(&producerLock);
    serverAssert(res == 0);
}

static void unlockProducerData() {
    int res = pthread_spin_unlock(&producerLock);
    serverAssert(res == 0);
}

static void freeArgAsSdsString(void *arg) {
    sdsfree(arg);
}

static sds marshall_for_no_exec(client *c) {
    serverAssert(strcasecmp(c->argv[0]->ptr, "exec") != 0);

    // node id
    uint8_t node_id = (uint8_t)server.node_id;
    sds buf = sdsnewlen(&node_id, sizeof(node_id));
    // client id
    uint64_t client_id = intrev64ifbe(c->id);
    buf = sdscatlen(buf, &client_id, sizeof(client_id));
    // db id
    uint8_t dbid = (uint8_t)c->db->id;
    buf = sdscatlen(buf, &dbid, sizeof(dbid));
    // command name
    serverAssert(c->argv[0]->encoding == OBJ_ENCODING_RAW || c->argv[0]->encoding == OBJ_ENCODING_EMBSTR);
    // right now Redis commands name is less than 255, check https://redis.io/commands
    serverAssert(sdslen(c->argv[0]->ptr) <= UINT8_MAX);     
    uint8_t cmd_len = sdslen(c->argv[0]->ptr);
    buf = sdscatlen(buf, &cmd_len, sizeof(cmd_len));
    buf = sdscatlen(buf, c->argv[0]->ptr, sdslen(c->argv[0]->ptr));
    // other arguments
    for (int i = 1; i < c->argc; ++i) {
        serverAssert(c->argv[i]->encoding == OBJ_ENCODING_RAW || c->argv[i]->encoding == OBJ_ENCODING_EMBSTR);
        serverAssert(sdslen(c->argv[i]->ptr) <= UINT32_MAX);
        uint32_t arg_len = intrev32ifbe(sdslen(c->argv[i]->ptr));
        buf = sdscatlen(buf, &arg_len, sizeof(arg_len));
        buf = sdscatlen(buf, c->argv[i]->ptr, sdslen(c->argv[i]->ptr));
    } 

    return buf;
}

// referece multi.c execCommand()
static sds marshall_for_exec(client *c) {
    serverAssert(strcasecmp(c->argv[0]->ptr, "exec") == 0);

    // node id
    uint8_t node_id = (uint8_t)server.node_id;
    sds buf = sdsnewlen(&node_id, sizeof(node_id));
    // client id
    uint64_t client_id = intrev64ifbe(c->id);
    buf = sdscatlen(buf, &client_id, sizeof(client_id));
    // db id
    uint8_t dbid = (uint8_t)c->db->id;
    buf = sdscatlen(buf, &dbid, sizeof(dbid));
    // command name
    serverAssert(c->argv[0]->encoding == OBJ_ENCODING_RAW || c->argv[0]->encoding == OBJ_ENCODING_EMBSTR);
    // right now Redis commands name is less than 255, check https://redis.io/commands
    serverAssert(sdslen(c->argv[0]->ptr) <= UINT8_MAX);     
    uint8_t cmd_len = 4;
    buf = sdscatlen(buf, &cmd_len, sizeof(cmd_len));
    buf = sdscatlen(buf, "exec", 4);
    // other arguments for multi commands in transaction
    serverAssert(c->mstate.count > 0);
    for (int i = 0; i < c->mstate.count; ++i) {
        // command name
        struct redisCommand *each_cmd = c->mstate.commands[i].cmd;
        uint8_t each_cmd_len = strlen(each_cmd->name);
        buf = sdscatlen(buf, &each_cmd_len, sizeof(each_cmd_len));
        buf = sdscatlen(buf, each_cmd->name, each_cmd_len);
        // args number (exclude command itself)
        serverAssert(c->mstate.commands[i].argc > 0);
        uint32_t each_args_num = intrev32ifbe(c->mstate.commands[i].argc - 1);
        buf = sdscatlen(buf, &each_args_num, sizeof(each_args_num));
        // each arg
        for (size_t j = 0; j < each_args_num; ++j) {
            robj *each_arg = c->mstate.commands[i].argv[j+1];
            uint32_t each_arg_len = intrev32ifbe(sdslen(each_arg->ptr));
            buf = sdscatlen(buf, &each_arg_len, sizeof(each_arg_len));
            buf = sdscatlen(buf, each_arg->ptr, sdslen(each_arg->ptr));
        }
    }

    return buf;
}

/* Main thread call this to add command to sndMsgs 
 * We think every parameter (including command name) stored in client argv is String Object */
static void addCommandToStreamWrite(client *c) {
    // serialize the command
    sds buf;
    if (c->flags & CLIENT_MULTI) {
        buf = marshall_for_exec(c);
    } else {
        buf = marshall_for_no_exec(c);
    }

    lockProducerData();
    listAddNodeTail(sndMsgs, buf);
    unlockProducerData();
}

/* C_OK, succesfully, C_ERR, failed */
static int parse_msg_for_no_exec(sds msg, uint8_t *node_id, uint64_t *client_id, uint8_t *dbid, 
                                 sds *command, list **args) {
    size_t msg_len = sdslen(msg);
    size_t cnt = 0;
    char *p = msg;

    // node id
    *node_id = *((uint8_t*)p);
    cnt += sizeof(*node_id);
    if (cnt > msg_len) return C_ERR;
    p += sizeof(*node_id);

    // client id
    *client_id = intrev64ifbe(*((uint64_t*)p));
    cnt += sizeof(*client_id);
    if (cnt > msg_len) return C_ERR;
    p += sizeof(*client_id);

    // db id
    *dbid = *((uint8_t*)p);
    cnt += sizeof(*dbid);
    if (cnt > msg_len) return C_ERR;
    p += sizeof(*dbid);

    // command name
    uint8_t cmd_len = *((uint8_t*)p);
    cnt += sizeof(cmd_len);
    if (cnt > msg_len) return C_ERR;
    p += sizeof(cmd_len);
    cnt += cmd_len;
    if (cnt > msg_len) return C_ERR;
    *command = sdsnewlen(p, cmd_len);
    p += cmd_len;

    // other arguments for no-exec commad
    list *other_args = NULL;
    if (cnt < msg_len) {
        other_args = listCreate();
        while (cnt < msg_len) {
            uint32_t arg_len = intrev32ifbe(*((uint32_t*)p));
            cnt += sizeof(arg_len);
            if (cnt > msg_len) return C_ERR;
            p += sizeof(arg_len);

            cnt += arg_len;
            if (cnt > msg_len) return C_ERR;
            sds arg = sdsnewlen(p, arg_len);
            p += arg_len;

            listAddNodeTail(other_args, arg);
        }
    }

    if (cnt != msg_len) return C_ERR;

    *args = other_args;
    return C_OK;
}

/* C_OK, succesfully, C_ERR, failed 
 * cmds is transaction commands in transaction, a list of sds
 * args is arguments which exclude the command name for each transaction command, a list of list of sds
 * the length of cmds is equal to the one of args. for the list of args for one command, it could be NULL 
 * NOTE: the length of cmds must be greater than 0, i.e., We do not accept empty transaction 
 * Reference marshall_for_exec() for encoding format */
static int parse_msg_for_exec(sds msg, uint8_t *node_id, uint64_t *client_id, uint8_t *dbid, 
                              list **cmds, list **args) {
    size_t msg_len = sdslen(msg);
    size_t cnt = 0;
    char *p = msg;

    // node id
    *node_id = *((uint8_t*)p);
    cnt += sizeof(*node_id);
    p += sizeof(*node_id);
    if (cnt > msg_len) return C_ERR;

    // client id
    *client_id = intrev64ifbe(*((uint64_t*)p));
    cnt += sizeof(*client_id);
    p += sizeof(*client_id);
    if (cnt > msg_len) return C_ERR;

    // db id
    *dbid = *((uint8_t*)p);
    cnt += sizeof(*dbid);
    p += sizeof(*dbid);
    if (cnt > msg_len) return C_ERR;

    // command name must be "exec"
    serverAssert(*((uint8_t*)p) == 4);
    cnt += 5;
    p += 5;
    if (cnt > msg_len) return C_ERR;

    // other commands and arguments for exec commad
    serverAssert(cnt != msg_len);
    list *tran_cmds = listCreate();
    list *tran_args = listCreate();
    while (cnt < msg_len) {
        // transaction command name
        uint8_t tran_cmd_len = *((uint8_t*)p);
        cnt += sizeof(tran_cmd_len);
        p += sizeof(tran_cmd_len);
        if (cnt > msg_len) return C_ERR;

        sds tran_cmd = sdsnewlen(p, tran_cmd_len);
        cnt += tran_cmd_len;
        p += tran_cmd_len;
        if (cnt > msg_len) return C_ERR;
        listAddNodeTail(tran_cmds, tran_cmd);

        // all args for each transaction command
        list *each_cmd_args = NULL;

        uint32_t each_cmd_arg_num = intrev32ifbe(*((uint32_t*)p));
        cnt += sizeof(each_cmd_arg_num);
        p += sizeof(each_cmd_arg_num);
        if (cnt > msg_len) return C_ERR;

        if (each_cmd_arg_num) {
            each_cmd_args = listCreate();
            for (size_t i = 0; i < each_cmd_arg_num; ++i) {
                uint32_t each_arg_len = intrev32ifbe(*((uint32_t*)p));
                cnt += sizeof(each_arg_len);
                p += sizeof(each_arg_len);
                if (cnt > msg_len) return C_ERR;
                sds each_cmd_arg = sdsnewlen(p, each_arg_len);
                cnt += each_arg_len;
                p += each_arg_len;
                if (cnt > msg_len) return C_ERR;
                listAddNodeTail(each_cmd_args, each_cmd_arg);
            }
        }

        listAddNodeTail(tran_args, each_cmd_args);
    }

    if (cnt != msg_len) {
        return C_ERR;
    } else {
        *cmds = tran_cmds;
        *args = tran_args;
        return C_OK;   
    } 
}

/* What is a virtual client ? 
 * please reference scripting.c
 * We create a virtual client which will execute a command in the virtual client context
 * for streamwrite which may come from other nodes's clients or 
 * the client in the node which issue a streamwrite comand then the connection is broken in asyinc mode */

/* reference server.c processCommand(). The caller is networing.c processInputBuffer()
 * return 
 *         STREAM_CHECK_GO_ON_NO_ERROR. The caller can go on to execute with rock key check
 *         STREAM_CHECK_GO_ON_WITH_ERROR. The caller can go on without rock key check
 *         STREAM_CHECK_ACL_FAIL. Alreay reply with ACL failed info.
 *         STREAM_CHECK_EMPTY_TRAN. The caller can go on. no need to check rock key and STREAM WAITING STATE
 *         STREAM_CHECK_SET_STREAM. Start the stream phase */
int checkAndSetStreamWriting(client *c) {
    // The caller is concrete client and the state is STREAM_WRITE_INIT
    serverAssert(c->streamWriting == STREAM_WRITE_INIT && c != server.virtual_client);

    // quit command is a shortcut
    if (!strcasecmp(c->argv[0]->ptr,"quit")) return STREAM_CHECK_GO_ON_NO_ERROR;

    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);

    // command name can not parse 
    if (!cmd) return STREAM_CHECK_GO_ON_WITH_ERROR;

    if (cmd->streamCmdCategory == STREAM_FORBIDDEN_CMD)
        return STREAM_CHECK_GO_ON_WITH_ERROR;

    // command basic parameters number is not OK
    if ((cmd->arity > 0 && cmd->arity != c->argc) ||
        (c->argc < -cmd->arity)) 
        return STREAM_CHECK_GO_ON_WITH_ERROR;
 
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
        if (!(cmd->flags & CMD_NO_AUTH)) return STREAM_CHECK_GO_ON_WITH_ERROR;
    }

    // check ACL, NOTE: we need to recover the c's cmd and lastcmd before return
    struct redisCommand *savedCmd = c->cmd;
    struct redisCommand *savedLastcmd = c->lastcmd;
    c->cmd = c->lastcmd = cmd;

    // ACL
    int acl_errpos;
    int acl_retval = ACLCheckAllPerm(c,&acl_errpos);
    if (acl_retval != ACL_OK) {
        switch (acl_retval) {
        case ACL_DENIED_CMD:
            rejectCommandFormat(c,
                "-NOPERM this user has no permissions to run "
                "the '%s' command or its subcommand", c->cmd->name);
            break;
        case ACL_DENIED_KEY:
            rejectCommandFormat(c,
                "-NOPERM this user has no permissions to access "
                "one of the keys used as arguments");
            break;
        case ACL_DENIED_CHANNEL:
            rejectCommandFormat(c,
                "-NOPERM this user has no permissions to access "
                "one of the channels used as arguments");
            break;
        default:
            rejectCommandFormat(c, "no permission");
            break;
        }

        c->cmd = savedCmd;
        c->lastcmd = savedLastcmd;
        return STREAM_CHECK_ACL_FAIL;
    }

    /* Only allow a subset of commands in the context of Pub/Sub if the
     * connection is in RESP2 mode. With RESP3 there are no limits. */
    if (((c->flags & CLIENT_PUBSUB) && c->resp == 2) &&
        c->cmd->proc != pingCommand &&
        c->cmd->proc != subscribeCommand &&
        c->cmd->proc != unsubscribeCommand &&
        c->cmd->proc != psubscribeCommand &&
        c->cmd->proc != punsubscribeCommand &&
        c->cmd->proc != resetCommand) {
        c->cmd = savedCmd;
        c->lastcmd = savedLastcmd;
        return STREAM_CHECK_GO_ON_WITH_ERROR;        
    }

    if ((c->flags & CLIENT_MULTI)) {
        // For transaction, we need special check. Even the command belongs to STREAM_ENABLED_CMD,
        // in transaction context, it could be queued to avoid setting STREAM_WRITE_WAITING
        if (cmd->proc != execCommand) {
            c->cmd = savedCmd;
            c->lastcmd = savedLastcmd;
            return STREAM_CHECK_GO_ON_NO_ERROR;       // command could be queued
        } else { 
            if ((c->flags & CLIENT_DIRTY_EXEC)) {
                c->cmd = savedCmd;
                c->lastcmd = savedLastcmd;
                return STREAM_CHECK_GO_ON_WITH_ERROR;       // exec will fail because of CLIENT_DIRTY_EXEC
            } else if (c->mstate.count == 0) {
                c->cmd = savedCmd;
                c->lastcmd = savedLastcmd;
                return STREAM_CHECK_EMPTY_TRAN;       // empty transaction, i.e., MULTI then EXEC
            }
        }
    }
    
    if (cmd->streamCmdCategory == STREAM_ENABLED_CMD) {
        // we set STREAM_WRITE_WAITING for the concrete caller
        c->streamWriting = STREAM_WRITE_WAITING;    
        // add command info as message to sndMsgs which will trigger stream write
        addCommandToStreamWrite(c);
        c->cmd = savedCmd;
        c->lastcmd = savedLastcmd;
        return STREAM_CHECK_SET_STREAM;
    } else {
        c->cmd = savedCmd;
        c->lastcmd = savedLastcmd;
        return STREAM_CHECK_GO_ON_NO_ERROR;     // e.g. read command
    }
}

/* set virtual client context for command, dbid and args 
 * so it can call execVritualCommand() directly later 
 * args is not like c->args, it exclude the first command name and could be NULL (no args)
 * and it is based on sds string not robj* */
/*
static void setVirtualClinetContextForNoTransaction(uint8_t dbid, sds command, list *args) {
    // setVirtualClinetContext() is not reentry if not called freeVirtualClientContext() before (or init)
    serverAssert(server.virtual_client->argv == NULL && server.virtual_client->argc == 0); 
    serverAssert(server.virtual_client->mstate.count == 0);       

    client *virtual = server.virtual_client;

    int select_res = selectDb(virtual, dbid);
    serverAssert(select_res == C_OK);

    size_t argc = 1 + (args ? listLength(args) : 0);
    virtual->argc = (int)argc;
    virtual->argv = zmalloc(sizeof(robj*)*argc);
    virtual->argv[0] = createStringObject(command, sdslen(command));

    if (args) {
        listIter li;
        listNode *ln;
        listRewind(args, &li);
        int index = 1;
        while ((ln = listNext(&li))) {
            sds arg = listNodeValue(ln);
            virtual->argv[index] = createStringObject(arg, sdslen(arg));
            ++index;
        }
    }
    
    struct redisCommand *cmd = lookupCommand(virtual->argv[0]->ptr);
    serverAssert(cmd);
    virtual->cmd = virtual->lastcmd = cmd;
}
*/

/* set virtual client context for command, dbid and args 
 * so it can call execVritualCommand() directly later 
 * args is not like c->args, it exclude the first command name and could be NULL (no args)
 * and it is based on sds string not robj* */
static void setVirtualClientContextForNoTransaction(uint8_t dbid, sds command, list *args) {
    // setVirtualClinetContext() is not reentry if not called freeVirtualClientContext() before (or init)
    serverAssert(server.virtual_client->argv == NULL && server.virtual_client->argc == 0);
    serverAssert(server.virtual_client->mstate.count == 0);

    client *c = server.virtual_client;

    int select_res = selectDb(c, dbid);
    serverAssert(select_res == C_OK);

    size_t argc = 1 + (args ? listLength(args) : 0);
    c->argc = (int)argc;
    c->argv = zmalloc(sizeof(robj*) * argc);
    c->argv[0] = createStringObject(command, sdslen(command));

    if (args) {
        listIter li;
        listNode *ln;
        listRewind(args, &li);
        size_t index = 1;
        while ((ln = listNext(&li))) {
            sds arg = listNodeValue(ln);
            c->argv[index] = createStringObject(arg, sdslen(arg));
            index++;
        }
    }    

    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);
    serverAssert(cmd);
    c->cmd = c->lastcmd = cmd;
}

static void setVirtualClinetContextForTransaction(uint8_t dbid, list *cmds, list *args) {
    serverAssert(server.virtual_client->argv == NULL && server.virtual_client->argc == 0);        
    serverAssert(server.virtual_client->mstate.count == 0);     
    serverAssert(cmds && args && listLength(cmds) > 0 && listLength(args) > 0);  
    serverAssert(listLength(cmds) == listLength(args));

    client *c = server.virtual_client;

    int select_res = selectDb(c, dbid);
    serverAssert(select_res == C_OK);

    c->flags |= CLIENT_MULTI;

    // only one argv which is exec string object
    c->argc = 1;
    c->argv = zmalloc(sizeof(robj*));
    c->argv[0] = createStringObject(sds_exec, sdslen(sds_exec));
    
    c->cmd = c->lastcmd = lookupCommand(sds_exec);  // must be exec commmand

    // for transaction context, refrerence multi.c queueMultiCommand()
    size_t multi_cmd_len = listLength(cmds);
    c->mstate.commands = zmalloc(sizeof(multiCmd)*multi_cmd_len);
    c->mstate.count = multi_cmd_len;

    listIter li_each_cmd;
    listNode *ln_each_cmd;
    listIter li_each_args;
    listNode *ln_each_args;
    listRewind(cmds, &li_each_cmd);
    listRewind(args, &li_each_args);
    sds each_cmd_sds;
    multiCmd *mc;
    for (size_t i = 0; i < multi_cmd_len; ++i) {
        mc = c->mstate.commands+i;

        ln_each_cmd = listNext(&li_each_cmd);
        serverAssert(ln_each_cmd);
        each_cmd_sds = listNodeValue(ln_each_cmd);
        serverAssert(each_cmd_sds);
        struct redisCommand *redis_cmd = lookupCommand(each_cmd_sds);
        serverAssert(redis_cmd);        

        mc->cmd = redis_cmd;
        c->mstate.cmd_flags |= redis_cmd->flags;
        c->mstate.cmd_inv_flags |= ~redis_cmd->flags;

        ln_each_args = listNext(&li_each_args);
        serverAssert(ln_each_args);
        list *each_args = listNodeValue(ln_each_args);
        if (each_args) serverAssert(listLength(each_args) > 0);
        mc->argc = 1 + (each_args ? listLength(each_args) : 0);
        mc->argv = zmalloc(sizeof(robj*) * mc->argc);

        mc->argv[0] = createStringObject(each_cmd_sds, sdslen(each_cmd_sds));

        if (each_args) {
            size_t index = 1;
            listIter li;
            listNode *ln;
            listRewind(each_args, &li);
            while ((ln = listNext(&li))) {
                sds arg = listNodeValue(ln);
                serverAssert(arg);
                mc->argv[index] = createStringObject(arg, sdslen(arg));
                ++index;
            }
        }
    }    
}

/* function like the above setVirtualClinetContextForTransaction() but it is called from 
 * So we can use the resource from concrete resource */
static void setVirtualClinetContextForNoTransactionFromConcrete(client *concrete) {
    serverAssert(server.virtual_client->argv == NULL && server.virtual_client->argc == 0);
    serverAssert(server.virtual_client->mstate.count == 0);       

    client *c = server.virtual_client;

    int select_res = selectDb(c, concrete->db->id);
    serverAssert(select_res == C_OK);

    c->argc = concrete->argc;
    c->argv = zmalloc(sizeof(robj*)*concrete->argc);
    for (int i = 0; i < concrete->argc; ++i) {
        robj *o = concrete->argv[i];
        c->argv[i] = o;
        incrRefCount(o);
    }
    c->cmd = c->lastcmd = concrete->cmd;
}

static void setVirtualClinetContextForTransactionFromConcrete(client *concrete) {
    serverAssert(server.virtual_client->argv == NULL && server.virtual_client->argc == 0);  
    serverAssert(server.virtual_client->mstate.count == 0);       

    client *c = server.virtual_client;

    int select_res = selectDb(c, concrete->db->id);
    serverAssert(select_res == C_OK);

    c->flags |= CLIENT_MULTI;

    serverAssert(concrete->argc == 1);
    c->argc = 1;
    c->argv = zmalloc(sizeof(robj*));
    robj *exec_cmd = concrete->argv[0];
    c->argv[0] = exec_cmd;
    incrRefCount(exec_cmd);
    serverAssert(concrete->cmd->proc == execCommand && concrete->lastcmd->proc == execCommand);
    c->cmd = c->lastcmd = concrete->cmd;
   
    // mstate
    serverAssert(concrete->mstate.count > 0);
    c->mstate.commands = zmalloc(sizeof(multiCmd) * concrete->mstate.count);
    for (int i = 0; i < concrete->mstate.count; ++i) {
        multiCmd *mc_virtual = c->mstate.commands + i;
        multiCmd *mc_concrete = concrete->mstate.commands + i;

        mc_virtual->argc = mc_concrete->argc;
        mc_virtual->cmd = mc_concrete->cmd;
        mc_virtual->argv = zmalloc(sizeof(robj*)*mc_concrete->argc);
        for (int j = 0; j < mc_concrete->argc; ++j) {
            robj *o = mc_concrete->argv[j];
            mc_virtual->argv[j] = o;
            incrRefCount(o);
        }
    }
    c->mstate.count = concrete->mstate.count;
    c->mstate.cmd_flags = concrete->mstate.cmd_flags;
    c->mstate.cmd_inv_flags = concrete->mstate.cmd_inv_flags;
}

/* when concreate client is destroyed and we find that the client is the current stream client, 
 * we need to transfer its context to virtual context.
 * Check network.c freeClient() for more details 
 * For stream phase, it is not necessary because all info can be constructed from stream message 
 * But for roch phase, it is essential because rock phasse with steam command execution
 * need an exection context which transfer from concrete client to virtual context */
void setVirtualContextFromConcreteClient(client *concrete) {
    serverAssert(concrete != server.virtual_client);
    serverAssert(server.streamCurrentClientId == concrete->id);
    // virtual_client right now is zero, i.e., not used by anyone
    serverAssert(server.virtual_client->argv == NULL && server.virtual_client->argc == 0);  
    serverAssert(concrete->argc > 0);
    serverAssert(concrete->streamWriting == STREAM_WRITE_WAITING || concrete->streamWriting == STREAM_WRITE_FINISH);

    // NOTE: We does not switch streamCurrentClientId to VIRTUAL_CLIENT_ID, 
    //       We need to keep server.streamCurrentClientId as before.
    //       In rock phase, when rock key is returned, we check all client ids of the key.
    //       Then we can compare them with streamCurrentClientId becasue stream write is one bye one. 
    //       If the before-concrete client id is kept in server.streamCurrentClientId, 
    //       we can know that the command of the before-concrete client is stream write or not.
    //       For no stream write command, we need to skip it because they are read commands. 
    //       But for stream write, we must do it even the concrete client has dropped the connection.
    //       Check checkAndSetRockKeyNumber() and rockReadSignalHandler in rock.c
    // *** server.streamCurrentClientId = VIRTUAL_CLIENT_ID; ***

    serverAssert(concrete->db->id >= 0 && concrete->db->id < server.dbnum);
    // uint8_t dbid = (uint8_t)concrete->db->id;

    if (concrete->mstate.count == 0) {
        setVirtualClinetContextForNoTransactionFromConcrete(concrete);
    } else {
        serverAssert(strcasecmp(concrete->argv[0]->ptr, "exec") == 0);

        setVirtualClinetContextForTransactionFromConcrete(concrete);
    }

    // do not forget to set the rockyNumber
    server.virtual_client->rockKeyNumber = concrete->rockKeyNumber;     
}

static void freeVirtualClientContext() {
    client* c = server.virtual_client;
    serverAssert(c->rockKeyNumber == 0);

    // deal with reply. We do not need any reply info
    while(listLength(c->reply)) {
        listDelNode(c->reply,listFirst(c->reply));
    }
    c->bufpos = 0;
    c->reply_bytes = 0;

    // deal with c->args
    for (int j = 0; j < c->argc; ++j) {
        robj* obj = c->argv[j];
        decrRefCount(obj);
    }
    zfree(c->argv);
    c->argc = 0;
    c->argv = NULL;

    c->cmd = c->lastcmd = NULL;

    // reference multi.c discardTransaction()
    if (c->mstate.count > 0)
        discardTransaction(c);
}

/* reference scripting.c luaRedisGenericCommand
 * right now for simplicity, we do not use cache
 * from networking.c processInlineBuffer() */
void execVirtualCommand() {
    /* Run the command */
    serverAssert(lookupStreamCurrentClient() == server.virtual_client);
    serverAssert(server.virtual_client->rockKeyNumber == 0);

    if (!(server.virtual_client->flags & CLIENT_MULTI)) {
        int call_flags = CMD_CALL_SLOWLOG | CMD_CALL_STATS;
        call(server.virtual_client, call_flags);
    } else {
        // Transaction need to call to multi.c 
        execCommand(server.virtual_client);
    }
    
    serverAssert((server.virtual_client->flags & CLIENT_BLOCKED) == 0);     // can not block virtual client
    serverAssert((server.virtual_client->flags & CLIENT_MULTI) == 0);       // after execution, multi will be cleared

    // clean up
    server.streamCurrentClientId = NO_STREAM_CLIENT_ID;     // for the next stream client
    freeVirtualClientContext();     // the matched clearing-up for setVirtualClientContext()

    // after the current stream write command finished, we need to go on for other stream commands
    // try_to_execute_stream_commands();
}

/*                                 */
/*                                 */
/* The following is about producer */
/*                                 */
/*                                 */

/* this is be called in produer thread. NOTE: we clear payload here 
 * reference https://github.com/edenhill/librdkafka/blob/master/examples/idempotent_producer.c */
static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
    UNUSED(rk);
    UNUSED(opaque);

    // NOTE: https://github.com/edenhill/librdkafka/issues/1288
    sds msg = rkmessage->_private;
    serverAssert(msg);
    
    if (rkmessage->err) {
        // permanent error
        serverLog(LL_WARNING, "%% Message delivery permantlly failed: %s\n", rd_kafka_err2str(rkmessage->err));
        exit(1);
    } else {
        sdsfree(msg);   // when success delivered, we free the msg
    }
}

/* reference https://github.com/edenhill/librdkafka/blob/master/examples/idempotent_producer.c */
static void error_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
    UNUSED(rk);
    UNUSED(opaque);

    rd_kafka_resp_err_t orig_err;
    char errstr[512];

    if (err != RD_KAFKA_RESP_ERR__FATAL) {
        // internal handle for gracefully retry by librdkafka library
        serverLog(LL_WARNING, "kafka producer no-fatel error, usually you do not care, error = %s, reason = %s",
            rd_kafka_err2name(err), reason);        
    } else {
        // fatel error
        orig_err = rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
        serverLog(LL_WARNING, "kafka producer fatel error, fatel error = %s, error = %s",
            rd_kafka_err2name(orig_err), errstr);
        exit(1);
    }
}

static sds pickSndMsgInProducerThread() {
    listIter li;
    listNode *ln;
    sds msg;

    lockProducerData();

    listRewind(sndMsgs, &li);
    ln = listNext(&li);    
    if (ln) {
        msg = listNodeValue(ln);
        listDelNode(sndMsgs, ln);
    } else {
        msg = NULL;
    }

    unlockProducerData();

    return msg;
}

/* reference https://github.com/edenhill/librdkafka/blob/master/examples/idempotent_producer.c */
static void sendKafkaMsgInProducerThread(sds msg, rd_kafka_t *rk, rd_kafka_topic_t *rkt) {
    serverAssert(msg);

    int errno;
    while (1) {
        // NOTE: msg_opaque parameter needs to be msg to reclame the memory in db_msg_cb()
        errno = rd_kafka_produce(rkt, 0, 0, msg, sdslen(msg), NULL, 0, msg);    

        switch (errno) {
        case 0:
            rd_kafka_poll(rk, 0/*non-blocking*/);
            return; // successfully
        case ENOBUFS:   
            // kafak producer queue is full
            rd_kafka_poll(rk, 1000/*block for max 1000ms*/);
            break;      // retry
        default:
            // stop error
            serverLog(LL_WARNING, "sendKafkaMsgInProducerThread() fatel error, errono = %d, reason = %s",
                errno, rd_kafka_err2name(rd_kafka_last_error()));
            exit(1);
        }
    }

    serverLog(LL_WARNING, "sendKafkaMsgInProducerThread() can not reach here!");
    exit(1);
}

/* We use rd_kafka_metadata() to check the topic valid in Kafka 
 * Most rdkafka API are local only, we need a API for test Kafka liveness */
void checkKafkaInProduerThread(rd_kafka_t *rk, rd_kafka_topic_t *rkt) {
    const struct rd_kafka_metadata *data;
    rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 1, rkt, &data, 50000);  // 5 seconds
    if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_metadata_destroy(data);
        lockProducerData();
        kafkaReadyAndTopicCorrect = 1;
        unlockProducerData();
    } else {
        serverLog(LL_WARNING, "checkKafkaInProduerThread() failed for reason = %s", rd_kafka_err2str(err));
    }
}

/* this is the producer thread entrance. Here we init kafka prodcer environment 
 * and then start a forever loop for send messages and deal with error */
static void* entryInProducerThread(void *arg) {
    UNUSED(arg);

    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt; 
    rd_kafka_conf_t *conf;
    rd_kafka_topic_conf_t *topicConf;
    char errstr[512];  

    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrapBrokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer failed for rd_kafka_conf_set() bootstrap.servers, reason = %s", errstr);
    if (rd_kafka_conf_set(conf, "linger.ms", "0",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer failed for rd_kafka_conf_set() linger.ms, reason = %s", errstr);
    // set time out to be infinite
    if (rd_kafka_conf_set(conf, "message.timeout.ms", "1",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer failed for rd_kafka_conf_set() message.timeout.ms, reason = %s", errstr);
    if (rd_kafka_conf_set(conf, "enable.idempotence", "true",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer failed for rd_kafka_conf_set() enable.idempotence, reason = %s", errstr);
    // set broker message size (to 100 M)
    if (rd_kafka_conf_set(conf, "message.max.bytes", "100000000",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer failed for rd_kafka_conf_set() message.max.bytes, reason = %s", errstr);
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
    rd_kafka_conf_set_error_cb(conf, error_cb);
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk)
        serverPanic("initKafkaProducer failed for rd_kafka_new(), reason = %s", errstr);
    topicConf = rd_kafka_topic_conf_new();
    if (!topicConf)
        serverPanic("initKafkaProducer failed for rd_kafka_topic_conf_new()");
    rkt = rd_kafka_topic_new(rk, bunnyRedisTopic, topicConf);
    if (!rkt)
        serverPanic("initKafkaProducer failed for rd_kafka_topic_new(), errno = %d,", errno);

    checkKafkaInProduerThread(rk, rkt); // for waiting mainthread

    sndMsgs = listCreate();
    uint sleepMicro = START_SLEEP_MICRO;      
    while(1) {
        sds msg = pickSndMsgInProducerThread();
        if (msg) {
            sendKafkaMsgInProducerThread(msg, rk, rkt);
            sleepMicro = START_SLEEP_MICRO;
            continue;
        }

        rd_kafka_poll(rk, 0/*non-blocking*/);
        usleep(sleepMicro);
        sleepMicro <<= 1;        // double sleep time
        if (sleepMicro > MAX_SLEEP_MICRO) 
            sleepMicro = MAX_SLEEP_MICRO;
    }

    return NULL;
}

/* we use only one thread for producer, rdkafka library use multithread internally.
 * but main thead need use spin lock to guarantee no context switch to kernel  
 * for kafka idompootent C code, reference https://github.com/edenhill/librdkafka/blob/master/examples/idempotent_producer.c */
void initKafkaProducer() {
    pthread_t producer_thread;

   int spin_init_res = pthread_spin_init(&producerLock, 0);
    if (spin_init_res != 0)
        serverPanic("Can not init producer spin lock, error code = %d", spin_init_res);

    if (pthread_create(&producer_thread, NULL, entryInProducerThread, NULL) != 0) 
        serverPanic("Unable to create a producer thread.");

    // we use up to 10 seconds which must more than checkKafkaInProduerThread() to get Kafka state
    int seconds = 0;
    while (seconds < 10) {
        lockProducerData();
        if (kafkaReadyAndTopicCorrect == 1) {
            unlockProducerData();
            break;
        }
        unlockProducerData();
        usleep(1000*1000);
        ++seconds;
        serverLog(LL_NOTICE, "Main thread waiting for Kafka producer thread reporting kafka state ...");
    }
    lockProducerData();
    if (kafkaReadyAndTopicCorrect != 1) {
        serverLog(LL_WARNING, "Main thread check kafka state result failed for %d seconds!", seconds);
        unlockProducerData();
        exit(1);
    } 
    unlockProducerData();
}    

/*                                 */
/*                                 */
/* The following is about consumer */
/*                                 */
/*                                 */

/* main thread will deal each commmand with a streamWrite message here 
 * all parameters are parsed from the strem write message */
static client *processNoExecCommandForStreamWrite(uint8_t node_id, uint64_t client_id, uint8_t dbid,
                                                  sds command, list *args) {
    serverAssert(server.streamCurrentClientId == NO_STREAM_CLIENT_ID);
    serverAssert(command);

    // server.streamCurrentClientId is changed first only here
    // If server.streamCurrentClientId == virtual_client->id, i.e, VIRTUAL_CLIENT_ID, it means
    // 1. the stream write is from other nodes
    // 2. the stream write is from the node but the client has dropped the connection before here
    // 
    // NOTE: If the concrete client right now is valid but later has been dropped, we 
    client *c = server.virtual_client;
    if (node_id == server.node_id) {
        dictEntry *de = dictFind(server.clientIdTable, (const void*)client_id);
        if (de) c = dictGetVal(de);     // found the concrete client
    }
    server.streamCurrentClientId = c->id;   // NOTE: could be concrete client id or vritual client id

    if (c != server.virtual_client) {
        serverAssert(c->streamWriting == STREAM_WRITE_WAITING);
        serverAssert(strcasecmp(c->argv[0]->ptr, command) == 0);
        serverAssert((size_t)c->argc == 1 + (args ? listLength(args) : 0));

        c->streamWriting = STREAM_WRITE_FINISH;

        checkAndSetRockKeyNumber(c, 1);        // after the stream phase, we can goon to rock phase
        if (c->rockKeyNumber == 0)
            // resume the excecution of concrete client and clear server.streamCurrentClient
            processCommandAndResetClient(c);        

    } else {
        setVirtualClientContextForNoTransaction(dbid, command, args);

        checkAndSetRockKeyNumber(c, 1);        // after the stream phase, we can goon to rock phase 
        if (c->rockKeyNumber == 0) {
            // resume the execution of virtual client and clear server.streamCurrentClient
            execVirtualCommand();      
        }
    }

    // free command (sds) and args (list)
    sdsfree(command);
    if (args) {
        serverAssert(listLength(args) > 0);
        listSetFreeMethod(args, freeArgAsSdsString);
        listRelease(args);
    }

    return c;
}

static client* processExecCommandForStreamWrite(uint8_t node_id, uint64_t client_id, uint8_t dbid,
                                                list *cmds, list *args) {
    serverAssert(server.streamCurrentClientId == NO_STREAM_CLIENT_ID);

    client *c = server.virtual_client;
    if (node_id == server.node_id) {
        dictEntry *de = dictFind(server.clientIdTable, (const void*)client_id);
        if (de) c = dictGetVal(de);     // found the concrete client
    }
    server.streamCurrentClientId = c->id;   // NOTE: could be concrete client id or vritual client id

    if (c != server.virtual_client) {
        serverAssert(c->streamWriting == STREAM_WRITE_WAITING);
        serverAssert(strcasecmp(c->argv[0]->ptr, "exec") == 0);
        serverAssert(c->argc == 1);

        c->streamWriting = STREAM_WRITE_FINISH;

        checkAndSetRockKeyNumber(c, 1);        // after the stream phase, we can goon to rock phase
        if (c->rockKeyNumber == 0)
            // resume the excecution of concrete client and clear server.streamCurrentClient
            processCommandAndResetClient(c);        

    } else {
        setVirtualClinetContextForTransaction(dbid, cmds, args);
        checkAndSetRockKeyNumber(c, 1);        // after the stream phase, we can goon to rock phase 
        if (c->rockKeyNumber == 0)
            // resume the execution of virtual client and clear server.streamCurrentClient
            execVirtualCommand();       
    }

    // clean up for cmds (list) and args (list of list)
    listSetFreeMethod(cmds, freeArgAsSdsString);
    listRelease(cmds);

    listIter li;
    listNode *ln;
    while ((ln = listNext(&li))) {
        list *each_args = listNodeValue(ln);
        // NOTE: each_args could be NULL for each_args
        if (each_args) {                    
            listSetFreeMethod(each_args, freeArgAsSdsString);
            listRelease(each_args);
        }
    }
    listRelease(args);

    return c;
}

static int check_exec_msg(sds msg) {
    // node id + client id + dbid + command len
    size_t offset = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint8_t);
    
    if (sdslen(msg) < offset + 4)
        return 0;

    size_t cmd_len = msg[offset-1];
    return cmd_len == 4 && msg[offset] == 'e' && msg[offset+1] == 'x' && msg[offset+2] == 'e' && msg[offset+3] == 'c';
}


/* When the concrete client finished a stream write command in network.c processCommandAndResetClient()
 * or the virtual client finished a stream write command in execVirtualCommand(),
 * or just the main thread got a message from stream write in streamConsumerSignalHandler()
 * they all need to call try_to_execute_stream_commands() because other stream commands are waiting on line.
 * In otherwords, when server.streamCurrentClientId set to NO_STREAM_CLIENT_ID, 
 * it means the stream write can go on */
void try_to_execute_stream_commands() {
    // if there is a stream write unfinished, just return
    if (server.streamCurrentClientId != NO_STREAM_CLIENT_ID) return;

    listIter msg_li;
    listNode *msg_ln;
    sds msg;
    uint8_t node_id;
    uint64_t client_id;
    uint8_t dbid;
    client *c;

    sds command = NULL;
    list *no_exec_args = NULL;

    list *exec_cmds = NULL;
    list *exec_args = NULL;

    while (1) {
        lockConsumerData();

        if(listLength(rcvMsgs) == 0) {
            unlockConsumerData();
            break;     // no message to deal with
        }

        listRewind(rcvMsgs, &msg_li);
        msg_ln = listNext(&msg_li);
        serverAssert(msg_ln);
        msg = listNodeValue(msg_ln);

        int is_exec_msg = check_exec_msg(msg);

        int parse_ret;
        if (!is_exec_msg) {
            // NOTE: parse_msg will allocate memory resource for command and no_exec_args and no_exec_args may be NULL
            parse_ret = parse_msg_for_no_exec(msg, &node_id, &client_id, &dbid, &command, &no_exec_args);
        } else {
            // NOTE: will allocate memory fro exec_cmds and exec_args
            parse_ret = parse_msg_for_exec(msg, &node_id, &client_id, &dbid, &exec_cmds, &exec_args);
        }
        if (parse_ret != C_OK) {
            serverLog(LL_WARNING, "fatel error for parse message!");
            exit(1);
        }

        // clean message and args memory resource
        sdsfree(msg);
        listDelNode(rcvMsgs, msg_ln); 

        unlockConsumerData();     

        // NOTE1: The allocated resource of command and args will be cleared
        // NOTE2: processNoExecCommandForStreamWrite() or processExecCommandForStreamWrite() 
        //        maybe call back, so we need it out of lock to aovid lock reentry.
        if (!is_exec_msg) {
            serverAssert(command);
            c = processNoExecCommandForStreamWrite(node_id, client_id, dbid, command, no_exec_args);
        } else {
            serverAssert(exec_cmds && exec_args);            
            c = processExecCommandForStreamWrite(node_id, client_id, dbid, exec_cmds, exec_args);
        }

        // if current stream client id is not finished, we need break
        if (server.streamCurrentClientId != NO_STREAM_CLIENT_ID) break;
        
        serverAssert(c->rockKeyNumber == 0);     
    }    
}

/* the event handler is executed from main thread, which is signaled by the pipe
 * from the rockdb thread. When it is called by the eventloop, there is 
 * a return result in rockJob */
static void streamConsumerSignalHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    UNUSED(fd);

    // clear pipe signal
    char tmpUseBuf[1];
    size_t n = read(server.stream_pipe_read, tmpUseBuf, 1);     
    serverAssert(n == 1);

    try_to_execute_stream_commands();
}

/* signal main thread which will call streamConsumerSignalHandler() */
static void signalMainThreadByPipeInConsumerThread() {
    char tempBuf[1] = "a";
    size_t n = write(server.stream_pipe_write, tempBuf, 1);
    serverAssert(n == 1);
}

static void addRcvMsgInConsumerThread(size_t len, void *payload) {    
    serverAssert(payload);

    lockConsumerData();

    sds copy = sdsnewlen(payload, len);
    listAddNodeTail(rcvMsgs, copy);
    if (listLength(rcvMsgs) >= RCV_MSGS_TOO_LONG)
        serverLog(LL_WARNING, "addRcvMsgInConsumerThread() rcvMsgs too long, list length = %ld", listLength(rcvMsgs));
        
    unlockConsumerData();
}

/* this is the consumer thread entrance */
static void* entryInConsumerThread(void *arg) {
    UNUSED(arg);

    rd_kafka_t *rk;          /* Consumer instance handle */
    rd_kafka_conf_t *conf;   /* Temporary configuration object */
    rd_kafka_resp_err_t err; /* librdkafka API error code */
    char errstr[512];        /* librdkafka API error reporting buffer */
    rd_kafka_topic_partition_list_t *subscription; /* Subscribed topics */
    rd_kafka_topic_partition_t *zeroPartion;

    rcvMsgs = listCreate();

    // generate uuid for groupid, so ervery consumer is a single group in kafka
    char uuid[37];
    uuid_t binuuid;
    uuid_generate_random(binuuid);
    uuid_unparse(binuuid, uuid);

    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrapBrokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaConsumer failed for rd_kafka_conf_set() bootstrap.servers, reason = %s", errstr);
    if (rd_kafka_conf_set(conf, "group.id", uuid, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaConsumer failed for rd_kafka_conf_set() group.id, reason = %s", errstr);
    if (rd_kafka_conf_set(conf, "allow.auto.create.topics", "false", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaConsumer failed for rd_kafka_conf_set() allow.auto.create.topics, reason = %s", errstr);
    // we can not retrieve message which is out of range
    if (rd_kafka_conf_set(conf, "auto.offset.reset", "error", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaConsumer failed for rd_kafka_conf_set() auto.offset.reset, reason = %s", errstr);
    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk)
        serverPanic("initKafkaConsumer failed for rd_kafka_new()");
    conf = NULL; /* Configuration object is now owned, and freed, by the rd_kafka_t instance. */
    rd_kafka_poll_set_consumer(rk);
 
    subscription = rd_kafka_topic_partition_list_new(1);
    zeroPartion = rd_kafka_topic_partition_list_add(subscription, bunnyRedisTopic, 0);
    zeroPartion->offset = START_OFFSET;
    /* Subscribe to the list of topics, NOTE: only one in the list of topics */
    err = rd_kafka_assign(rk, subscription);
    if (err) 
        serverPanic("initKafkaConsumer failed for rd_kafka_subscribe() reason = %s", rd_kafka_err2str(err));
    rd_kafka_topic_partition_list_destroy(subscription);

    while(1) {
        rd_kafka_message_t *rkm = rd_kafka_consumer_poll(rk, 100);

        if (!rkm) continue;     // timeout with no message 

        if (rkm->err) {
            if (rkm->err == RD_KAFKA_RESP_ERR__AUTO_OFFSET_RESET) {
                serverLog(LL_WARNING, "offset %d for Broker: Offset out of range!!!", START_OFFSET);
                exit(1);
            }
            serverLog(LL_WARNING, "Consumer error(but Kafka can handle): err = %d, reason = %s", 
                      rkm->err, rd_kafka_message_errstr(rkm));
            rd_kafka_message_destroy(rkm);
            continue;
        }

        // proper message
        serverAssert(strcmp(rd_kafka_topic_name(rkm->rkt), bunnyRedisTopic) == 0 && rkm->len > 0);
        addRcvMsgInConsumerThread(rkm->len, rkm->payload);
        // notify main thread to perform streamConsumerSignalHandler()
        signalMainThreadByPipeInConsumerThread();   
    
        rd_kafka_message_destroy(rkm);
    }

    return NULL;
}

/* the function create a stream reader thread, which will read data from kafka 
 * when stream reader thread read some write operations from kafka,
 * it will write it to buffer (sync by spin lock) then notify main thread by pipe 
 * because main thread maybe sleep in eventloop for events comming */
void initStreamPipeAndStartConsumer() {
    sds_exec = sdsnewlen("exec", 4);        // init gloabl constant string for command "exec"

    pthread_t consumer_thread;
    int pipefds[2];

    int spin_init_res = pthread_spin_init(&consumerLock, 0);
    if (spin_init_res != 0)
        serverPanic("Can not init consumer spin lock, error code = %d", spin_init_res);

    if (pipe(pipefds) == -1) 
        serverPanic("Can not create pipe for stream.");

    server.stream_pipe_read = pipefds[0];
    server.stream_pipe_write = pipefds[1];

    if (aeCreateFileEvent(server.el, server.stream_pipe_read, 
        AE_READABLE, streamConsumerSignalHandler,NULL) == AE_ERR) {
        serverPanic("Unrecoverable error creating server.stream_pipe file event.");
    }

    if (pthread_create(&consumer_thread, NULL, entryInConsumerThread, NULL) != 0) {
        serverPanic("Unable to create a consumer thread.");
    }
}

