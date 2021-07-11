/* BunnyRedis is based on Redis, coded by Tony. The copyright is same as Redis.
 *
 * Copyright (c) 2018, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"

#include "endianconv.h"
#include "rock.h"
#include "streamwrite.h"
#include "zk.h"

#include <librdkafka/rdkafka.h>
#include <uuid/uuid.h>



// spin lock only run in Linux
static pthread_spinlock_t consumerLock;     
static pthread_spinlock_t producerLock;

#define START_SLEEP_MICRO   16
#define MAX_SLEEP_MICRO     1024    // max sleep for 1 ms

#define START_OFFSET        0       // consumer start offset to read

#define RCV_MSGS_TOO_LONG   1000    // if we recevice to much messages and not quick to consume, it just warns

#define MARSHALL_SIZE_OVERFLOW (64<<20)     // the max messsage size (NOTE: approbally)

static int kafkaReadyAndTopicCorrect = 0;   // producer startup check kafka state and main thread wait it for correctness
redisAtomic int kafkaStartupConsumeFinish;   // when startup, finish all consume data, then unpause all clients to start working
static const char *bootstrapBrokers = NULL;
static const char *bunnyRedisTopic = "redisStreamWrite"; 

static list* sndMsgs;  // producer and main thread contented data
static list* rcvMsgs;  // consumer and main thread contented data

static sds sds_exec;    // global variable which is "exec" string

void lockConsumerData() {
    int res = pthread_spin_lock(&consumerLock);
    serverAssert(res == 0);
}

void unlockConsumerData() {
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

/* before send, we need check the message size. 
 * Only need check agument because command name and other argument is small */
static int is_overflow_before_marshall_no_exec(client *c) {
    size_t total = 0;
    for (int i = 1; i < c->argc; ++i) {
        total += sdslen(c->argv[i]->ptr);
    }

    if (total > MARSHALL_SIZE_OVERFLOW)
        serverLog(LL_WARNING, "is_overflow_before_marshall_no_exec() found the command %s, too large with size = %ld",
                  (sds)c->argv[0]->ptr, total);

    return total > MARSHALL_SIZE_OVERFLOW;
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
    uint8_t exec_cmd_len = 4;
    buf = sdscatlen(buf, &exec_cmd_len, sizeof(exec_cmd_len));
    buf = sdscatlen(buf, "exec", 4);
    // other arguments for multi commands in transaction
    serverAssert(c->mstate.count > 0);
    for (int i = 0; i < c->mstate.count; ++i) {
        // command name
        struct redisCommand *each_cmd = c->mstate.commands[i].cmd;
        serverAssert(strlen(each_cmd->name) <= UINT8_MAX);
        uint8_t each_cmd_len = strlen(each_cmd->name);
        buf = sdscatlen(buf, &each_cmd_len, sizeof(each_cmd_len));
        buf = sdscatlen(buf, each_cmd->name, each_cmd_len);
        // args number (NOTE: exclude the first argument which is the command itself)
        serverAssert(c->mstate.commands[i].argc > 0);
        uint32_t each_args_num = intrev32ifbe(c->mstate.commands[i].argc - 1);
        buf = sdscatlen(buf, &each_args_num, sizeof(each_args_num));
        // each arg
        for (int j = 1; j < c->mstate.commands[i].argc; ++j) {
            robj *each_arg = c->mstate.commands[i].argv[j];
            uint32_t each_arg_len = intrev32ifbe(sdslen(each_arg->ptr));
            buf = sdscatlen(buf, &each_arg_len, sizeof(each_arg_len));
            buf = sdscatlen(buf, each_arg->ptr, sdslen(each_arg->ptr));
        }
    }

    return buf;
}

/* before send, we need check the message size 
 * Only need check agument because command name and other argument is small */
static int is_overflow_before_marshall_exec(client *c) {
    size_t total = 0;
    for (int i = 0; i < c->mstate.count; ++i) {
        for (int j = 1; j < c->mstate.commands[i].argc; ++j) {
            robj *each_arg = c->mstate.commands[i].argv[j];
            total += sdslen(each_arg->ptr);
        }
    }

    if (total > MARSHALL_SIZE_OVERFLOW)
        serverLog(LL_WARNING, "is_overflow_before_marshall_exec() found the command %s, too large with size = %ld",
                  "multi transaction", total);

    return total > MARSHALL_SIZE_OVERFLOW;
}

/* Main thread call this to add command to sndMsgs 
 * We think every parameter (including command name) stored in client argv is String Object */
static void addCommandToStreamWrite(client *c) {
    // it is wrong if sending msg when consumer is in startup state 
    serverAssert(server.node_id != CONSUMER_STARTUP_NODE_ID);   

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
    unsigned char *p = (unsigned char*)msg;

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
    unsigned char *p = (unsigned char*)msg;

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

static int check_nx_xx_for_set_comomand(client *c) {
    serverAssert(server.kafka_compcation);
    serverAssert(strcasecmp(c->argv[0]->ptr, "set") == 0);

    for (int i = 3; i < c->argc; ++i) {
        sds arg = c->argv[i]->ptr;
        if (strcasecmp(arg, "nx") == 0) {
            return 1;
        } else if (strcasecmp(arg, "xx") == 0) {
            return 1;
        }
    }
    return 0;
}

/* reference https://redis.io/commands#pubsub */
static int is_pubsub_cmd(char *cmd) {
    if (strcasecmp(cmd, "psubscribe") == 0) {
        return 1;
    } else if (strcasecmp(cmd, "pubsub") == 0) {
        return 1;
    } else if (strcasecmp(cmd, "publish") == 0) {
        return 1;
    } else if (strcasecmp(cmd, "punsubscribe") == 0) {
        return 1;
    } else if (strcasecmp(cmd, "subscribe") == 0) {
        return 1;
    } else if (strcasecmp(cmd, "unsubscribe") == 0) {
        return 1;
    } else {
        return 0;
    }
}

static int is_forbidden_cmd_in_compaction(char *cmd) {
    serverAssert(server.kafka_compcation);

    // check cal_kafka_key_for_xxx to see which commands are not forbidden in compaction mode
    if (strcasecmp(cmd, "del") == 0) {
        return 0;
    } else if (strcasecmp(cmd, "getdel") == 0) {
        return 0;
    } else if (strcasecmp(cmd, "getset") == 0) {
        return 0;
    } else if (strcasecmp(cmd, "hdel") == 0) {
        return 0;
    } else if (strcasecmp(cmd, "hmset") == 0) {
        return 0;
    } else if (strcasecmp(cmd, "hset") == 0) {
        return 0;
    } else if (strcasecmp(cmd, "set") == 0) {
        return 0;       // NOTE: set need further check for nx and xx
    } else if (strcasecmp(cmd, "unlink") == 0) {
        return 0;
    } else if (strcasecmp(cmd, "acl") == 0) {
        return 0;
    } else if (strcasecmp(cmd, "flushall") == 0) {
        return 0;
    } else if (strcasecmp(cmd, "flushdb") == 0) {
        return 0;
    } else if (is_pubsub_cmd(cmd)) {
        return 0;
    } else {
        return 1;
    }
}

/* What is a virtual client ? 
 * please reference scripting.c
 * We create a virtual client which will execute a command in the virtual client context
 * for streamwrite which may come from other nodes's clients or 
 * the client in the node which issue a streamwrite comand then the connection is broken in asyinc mode */

/* reference server.c processCommand(). The caller is networing.c processInputBuffer()
 * return 
 *         STREAM_CHECK_FORBIDDEN.   The command is forbidden by BunnyRedis
 *         STREAM_CHECK_GO_ON_NO_ERROR. The caller can go on to execute with rock key check
 *         STREAM_CHECK_GO_ON_WITH_ERROR. The caller can go on without rock key check
 *         STREAM_CHECK_ACL_FAIL. Alreay reply with ACL failed info.
 *         STREAM_CHECK_EMPTY_TRAN. The caller can go on. no need to check rock key and STREAM WAITING STATE
 *         STREAM_CHECK_SET_STREAM. Start the stream phase 
 * NOTE: special for set command. If it has expire argument, it will be determined as forbidden */
#define OBJ_NO_FLAGS    0       // check t_string.c, it is duplicated
#define COMMAND_SET     1       // check t_string.c, it is duplicated
int checkAndSetStreamWriting(client *c) {
    // The caller is concrete client and the state is STREAM_WRITE_INIT
    serverAssert(c->streamWriting == STREAM_WRITE_INIT && c != server.virtual_client);

    // quit command is a shortcut
    if (!strcasecmp(c->argv[0]->ptr,"quit")) return STREAM_CHECK_GO_ON_NO_ERROR;

    struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);

    // command name can not parse 
    if (!cmd) return STREAM_CHECK_GO_ON_WITH_ERROR;

    if (cmd->streamCmdCategory == STREAM_FORBIDDEN_CMD) {
        rejectCommandFormat(c,"BunnyRedis forbids '%s' command", c->argv[0]->ptr);
        return STREAM_CHECK_FORBIDDEN;
    } else if (cmd->streamCmdCategory == STREAM_NO_NEED_CMD) {
        // if the command is not a stream command 
        // e.g. all read commands or single node command like config, dbsize, connection, 
        // just go on
        return STREAM_CHECK_GO_ON_NO_ERROR;
    }

    // command basic parameters number is not OK
    if ((cmd->arity > 0 && cmd->arity != c->argc) ||
        (c->argc < -cmd->arity)) 
        return STREAM_CHECK_GO_ON_WITH_ERROR;

    // check set command has expire argument
    if (strcasecmp(c->argv[0]->ptr, "set") == 0) {
        robj *expire = NULL;
        int unit = UNIT_SECONDS;
        int flags = OBJ_NO_FLAGS;

        int parse_ret =  parseExtendedStringArgumentsWithoutReply(c,&flags,&unit,&expire,COMMAND_SET);
        
        if (parse_ret != C_OK) 
            return STREAM_CHECK_GO_ON_WITH_ERROR;        
    
        if (expire) {
            rejectCommandFormat(c,"BunnyRedis forbids '%s' command partially because the expire arguments", c->argv[0]->ptr);
            return STREAM_CHECK_FORBIDDEN;
        }

        // for set we can not use "nx" or "xx" arg with set
        if (server.kafka_compcation) {
            if (check_nx_xx_for_set_comomand(c)) {
                rejectCommandFormat(c,"BunnyRedis forbids '%s' command with nx or xx argument because kafka compact enabled", c->argv[0]->ptr);
                return STREAM_CHECK_FORBIDDEN;
            }
        }
    }

    // check whether support kafka compaction, if enable compaction, some commands are forbidden
    if (server.kafka_compcation) {
        if (is_forbidden_cmd_in_compaction(c->argv[0]->ptr)) {
            rejectCommandFormat(c,"BunnyRedis forbids '%s' command because kafka compact enabled", c->argv[0]->ptr);
            return STREAM_CHECK_FORBIDDEN;
        }
    }
 
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
        if (!(cmd->flags & CMD_NO_AUTH)) 
            return STREAM_CHECK_GO_ON_WITH_ERROR;
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
                "the '%s' command or its subcommand", c->argv[0]->ptr);
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
    // recover.   
    c->cmd = savedCmd;
    c->lastcmd = savedLastcmd;
    // NOTE: the following code can not use c->cmd and c->lastcmd anymore

    /* Only allow a subset of commands in the context of Pub/Sub if the
     * connection is in RESP2 mode. With RESP3 there are no limits. */
    if (((c->flags & CLIENT_PUBSUB) && c->resp == 2) &&
        cmd->proc != pingCommand &&
        cmd->proc != subscribeCommand &&
        cmd->proc != unsubscribeCommand &&
        cmd->proc != psubscribeCommand &&
        cmd->proc != punsubscribeCommand &&
        cmd->proc != resetCommand) {
        return STREAM_CHECK_GO_ON_WITH_ERROR;        
    }

    if ((c->flags & CLIENT_MULTI)) {
        // For transaction, we need special check. Even the command belongs to STREAM_ENABLED_CMD,
        // in transaction context, it could be queued to avoid setting STREAM_WRITE_WAITING
        // in transaction mode, we need to check queued command for kafka compaction
        if (cmd->proc != execCommand) {
            return STREAM_CHECK_GO_ON_NO_ERROR;       // command could be queued
        } else { 
            if ((c->flags & CLIENT_DIRTY_EXEC)) {
                return STREAM_CHECK_GO_ON_WITH_ERROR;       // exec will fail because of CLIENT_DIRTY_EXEC
            } else if (c->mstate.count == 0) {
                return STREAM_CHECK_EMPTY_TRAN;       // empty transaction, i.e., MULTI then EXEC
            }
        }
    }
    
    if (cmd->streamCmdCategory == STREAM_ENABLED_CMD) {
        // before setting STREAM_WRITE state， we need to check whether the message size is overflow
        int is_overflow = 0;
        if (c->flags & CLIENT_MULTI) {
            is_overflow = is_overflow_before_marshall_exec(c);
        } else {
            is_overflow = is_overflow_before_marshall_no_exec(c);
        }
        if (is_overflow) {
            rejectCommandFormat(c, "command with arguments is too large!");
            return STREAM_CHECK_MSG_OVERFLOW;
        }

        // we set STREAM_WRITE_WAITING for the concrete caller
        c->streamWriting = STREAM_WRITE_WAITING;    
        // add command info as message to sndMsgs which will trigger stream write
        addCommandToStreamWrite(c);
        return STREAM_CHECK_SET_STREAM;
    } else {
        return STREAM_CHECK_GO_ON_NO_ERROR;     // e.g. read command
    }
}

/* set virtual client context for command, dbid and args 
 * so it can call execVritualCommand() directly later 
 * args is not like c->args, it exclude the first command name and could be NULL (no args)
 * and it is based on sds string not robj* */
static void setVirtualClientContextForNoTransaction(uint8_t dbid, sds command, list *args) {
    // setVirtualClinetContext() is not reentry if not called freeVirtualClientContext() before (or init)
    serverAssert(server.virtual_client->argv == NULL && server.virtual_client->argc == 0);
    serverAssert(server.virtual_client->mstate.count == 0);

    client *v = server.virtual_client;

    int select_res = selectDb(v, dbid);
    serverAssert(select_res == C_OK);

    size_t argc = 1 + (args ? listLength(args) : 0);
    v->argc = (int)argc;
    v->argv = zmalloc(sizeof(robj*) * argc);
    v->argv[0] = createStringObject(command, sdslen(command));

    if (args) {
        listIter li;
        listNode *ln;
        listRewind(args, &li);
        size_t index = 1;
        while ((ln = listNext(&li))) {
            sds arg = listNodeValue(ln);
            v->argv[index] = createStringObject(arg, sdslen(arg));
            index++;
        }
    }    

    struct redisCommand *cmd = lookupCommand(v->argv[0]->ptr);
    serverAssert(cmd);
    v->cmd = v->lastcmd = cmd;
}

static void setVirtualClinetContextForTransaction(uint8_t dbid, list *cmds, list *args) {
    serverAssert(server.virtual_client->argv == NULL && server.virtual_client->argc == 0);        
    serverAssert(server.virtual_client->mstate.count == 0);     
    serverAssert(cmds && args && listLength(cmds) > 0 && listLength(args) > 0);  
    serverAssert(listLength(cmds) == listLength(args));

    client *v = server.virtual_client;

    int select_res = selectDb(v, dbid);
    serverAssert(select_res == C_OK);

    v->flags |= CLIENT_MULTI;

    // only one argv which is exec string object
    v->argc = 1;
    v->argv = zmalloc(sizeof(robj*));
    v->argv[0] = createStringObject(sds_exec, sdslen(sds_exec));
    
    v->cmd = v->lastcmd = lookupCommand(sds_exec);  // must be exec commmand

    // for transaction context, refrerence multi.c queueMultiCommand()
    size_t multi_cmd_len = listLength(cmds);
    v->mstate.commands = zmalloc(sizeof(multiCmd)*multi_cmd_len);
    v->mstate.count = multi_cmd_len;

    listIter li_each_cmd;
    listNode *ln_each_cmd;
    listIter li_each_args;
    listNode *ln_each_args;
    listRewind(cmds, &li_each_cmd);
    listRewind(args, &li_each_args);
    sds each_cmd_sds;
    multiCmd *mc;
    for (size_t i = 0; i < multi_cmd_len; ++i) {
        mc = v->mstate.commands+i;

        ln_each_cmd = listNext(&li_each_cmd);
        serverAssert(ln_each_cmd);
        each_cmd_sds = listNodeValue(ln_each_cmd);
        serverAssert(each_cmd_sds);
        struct redisCommand *redis_cmd = lookupCommand(each_cmd_sds);
        serverAssert(redis_cmd);        

        mc->cmd = redis_cmd;
        v->mstate.cmd_flags |= redis_cmd->flags;
        v->mstate.cmd_inv_flags |= ~redis_cmd->flags;

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
    serverAssert(!(concrete->flags & CLIENT_MULTI) && concrete->mstate.count == 0);

    client *v = server.virtual_client;

    int select_res = selectDb(v, concrete->db->id);
    serverAssert(select_res == C_OK);

    serverAssert(concrete->argc > 0);
    v->argc = concrete->argc;
    v->argv = zmalloc(sizeof(robj*)*concrete->argc);
    for (int i = 0; i < concrete->argc; ++i) {
        robj *o = concrete->argv[i];
        v->argv[i] = o;
        incrRefCount(o);
    }
    // NOTE: concrete cmd may be not set, we need to lookup and set
    struct redisCommand *cmd = lookupCommand(concrete->argv[0]->ptr);
    serverAssert(cmd);
    v->cmd = v->lastcmd = cmd;
}

static void setVirtualClinetContextForTransactionFromConcrete(client *concrete) {
    serverAssert(server.virtual_client->argv == NULL && server.virtual_client->argc == 0);  
    serverAssert(server.virtual_client->mstate.count == 0);   
    serverAssert(concrete->flags & CLIENT_MULTI && concrete->mstate.count > 0);    

    client *v = server.virtual_client;

    int select_res = selectDb(v, concrete->db->id);
    serverAssert(select_res == C_OK);

    serverAssert(concrete->flags & CLIENT_MULTI);
    v->flags = concrete->flags;

    serverAssert(concrete->argc == 1);
    v->argc = 1;
    v->argv = zmalloc(sizeof(robj*));
    robj *exec_obj = concrete->argv[0];
    serverAssert(exec_obj->type == OBJ_STRING && strcasecmp(exec_obj->ptr, "exec") == 0);
    v->argv[0] = exec_obj;
    incrRefCount(exec_obj);
    // NOTE: concrete->cmd may not be valid
    v->cmd = v->lastcmd = lookupCommand(sds_exec);
   
    // mstate
    v->mstate.commands = zmalloc(sizeof(multiCmd) * concrete->mstate.count);
    v->mstate.count = concrete->mstate.count;
    for (int i = 0; i < concrete->mstate.count; ++i) {
        multiCmd *mc_virtual = v->mstate.commands + i;
        multiCmd *mc_concrete = concrete->mstate.commands + i;

        mc_virtual->argc = mc_concrete->argc;
        mc_virtual->cmd = mc_concrete->cmd;
        mc_virtual->argv = zmalloc(sizeof(robj*)*mc_concrete->argc);
        for (int j = 0; j < mc_concrete->argc; ++j) {
            robj *o = mc_concrete->argv[j];
            serverAssert(o->type == OBJ_STRING);
            mc_virtual->argv[j] = o;
            incrRefCount(o);
        }
    }
    v->mstate.cmd_flags = concrete->mstate.cmd_flags;
    v->mstate.cmd_inv_flags = concrete->mstate.cmd_inv_flags;
}

/* when concreate client is destroyed and we find that the client is the current stream client, 
 * we need to transfer its context to virtual context.
 * Check network.c freeClient() for more details 
 * For stream phase, it is not necessary because all info can be constructed from stream message 
 * But for roch phase, it is essential because rock phasse with steam command execution
 * need an exection context which transfer from concrete client to virtual context */
void setVirtualContextFromWillFreeConcreteClient(client *concrete) {
    // it is for debug
    if (server._debug_)
        serverLog(LL_WARNING, "setVirtualContextFromConcreteClient(), call %s",
                concrete->mstate.count == 0 ? "no tran" : "tran");

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
    /* Run the stream write command */
    serverAssert(lookupStreamCurrentClient() == server.virtual_client);
    serverAssert(server.virtual_client->rockKeyNumber == 0);

    // debug
    if (server._debug_) {
        client *c = server.virtual_client;
        serverLog(LL_WARNING, "argc = %d, argv[0] = %s", c->argc, (sds)c->argv[0]->ptr);
        serverLog(LL_WARNING, "c->cmd = %s", c->cmd->name);
    }

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
}

/* consumer and producer both call check_exec_msg() */
static int check_exec_msg(sds msg) {
    size_t offset = sizeof(uint8_t) + sizeof(uint64_t) + 
                    sizeof(uint8_t) + sizeof(uint8_t);
    
    if (sdslen(msg) < offset + 4)
        return 0;

    size_t cmd_len = msg[offset-1];
    return cmd_len == 4 && msg[offset] == 'e' && 
           msg[offset+1] == 'x' && msg[offset+2] == 'e' && msg[offset+3] == 'c';
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
        serverPanic("%% Message delivery permantlly failed: %s\n", rd_kafka_err2str(rkmessage->err));
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
        serverPanic("kafka producer fatel error, fatel error = %s, error = %s",
                    rd_kafka_err2name(orig_err), errstr);
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

#define KAFKA_COMPACT_PK                255
#define KAFKA_COMPACT_SET_COMMAND       0
#define KAFKA_COMPACT_GETSET_COMMAND    1
#define KAFKA_COMPACT_GETDEL_COMMAND    2
#define KAFKA_COMPACT_HSET_COMMAND      3
#define KAFKA_COMPACT_HMSET_COMMAND     4
#define KAFKA_COMPACT_HDEL_COMMAND      5
#define KAFKA_COMPACT_DEL_COMMAND       6
#define KAFKA_COMPACT_UNLINK_COMMAND    7

/* for set redis command, no nx or xx, check https://redis.io/commands/set 
 * NOTE: it needs guarantee that: the set comannd with args will be executed successfully, i.e., no error.
 *       otherwise, it will break the integrity of data when compaction in Kafka. 
 *       How we guarantee that? Three mehods:
 *       1. in checkAndSetStreamWriting(), we exclude TTL
 *       2. right here, we execlude nx and xx
 *       3. in checkAndSetStreamWriting() we copy a lot of code in processCommand() 
 *          which check a lot of error before stream write */
static sds cal_kafka_key_for_set(uint8_t dbid, list *args) {
    serverAssert(server.kafka_compcation);

    serverAssert(listLength(args) >= 2);
    sds key = NULL;
    int exist_nx_or_xx = 0;
    listIter li;
    listNode *ln;
    listRewind(args, &li);
    int index = 0;

    while ((ln = listNext(&li))) {
        sds arg = listNodeValue(ln);
        if (index == 0) {
            key = arg;
        } else if (index > 1) {
            if (strcasecmp(arg, "nx") == 0) {
                exist_nx_or_xx = 1;
                break;
            } else if (strcasecmp(arg, "xx") == 0) {
                exist_nx_or_xx = 1;
                break;
            }
        }
        ++index;
    }

    // NOTE: checkAndSetStreamWriting() already forbid nx or xxs
    serverAssert(!exist_nx_or_xx);      

    if (!exist_nx_or_xx) {
        serverAssert(key);
        sds kafka_key = sdsnewlen(&dbid, sizeof(dbid));
        uint8_t kafka_cmd_id = KAFKA_COMPACT_SET_COMMAND;
        kafka_key = sdscatlen(kafka_key, &kafka_cmd_id, sizeof(kafka_cmd_id));
        kafka_key = sdscatlen(kafka_key, key, sdslen(key));
        return kafka_key;
    } else {
        return NULL;
    }
}

static sds cal_kafka_key_for_getset(uint8_t dbid, list *args) {
    serverAssert(server.kafka_compcation);
    serverAssert(listLength(args) == 2);

    sds key = listNodeValue(listIndex(args, 0));

    sds kafka_key = sdsnewlen(&dbid, sizeof(dbid));
    uint8_t kafka_cmd_id = KAFKA_COMPACT_GETSET_COMMAND;
    kafka_key = sdscatlen(kafka_key, &kafka_cmd_id, sizeof(kafka_cmd_id));
    kafka_key = sdscatlen(kafka_key, key, sdslen(key));

    return kafka_key;
}

static sds cal_kafka_key_for_getdel(uint8_t dbid, list *args) {
    serverAssert(server.kafka_compcation);
    serverAssert(listLength(args) == 1);

    sds key = listNodeValue(listIndex(args, 0));

    sds kafka_key = sdsnewlen(&dbid, sizeof(dbid));
    uint8_t kafka_cmd_id = KAFKA_COMPACT_GETDEL_COMMAND;
    kafka_key = sdscatlen(kafka_key, &kafka_cmd_id, sizeof(kafka_cmd_id));
    kafka_key = sdscatlen(kafka_key, key, sdslen(key));

    return kafka_key;
}

#define SPECIAL_CHAR_CAN_NOT_IN_STRING  254     // reference https://kb.iu.edu/d/aepu
static int has_special_char_in_key_or_field(sds check) {
    for (size_t i = 0; i < sdslen(check); ++i) {
        unsigned char c = check[i];
        if (c == SPECIAL_CHAR_CAN_NOT_IN_STRING)
            return 1;
    }
    return 0;
}

/* hset: Two conditions for hset command to kafka key.
 *       1. we only support one field 
 *       2. key and field can not have special charactor in the view of latin_1 encoding */
static sds cal_kafka_key_for_hset(uint8_t dbid, list *args) {
    serverAssert(server.kafka_compcation);

    size_t len = listLength(args);
    serverAssert(len % 2 == 1 && len >= 3);
    
    if (len != 3) 
        return NULL;    // only support one key with one field 

    sds key = listNodeValue(listIndex(args, 0));
    sds field = listNodeValue(listIndex(args, 1));

    if (has_special_char_in_key_or_field(key) || has_special_char_in_key_or_field(field))
        return NULL;

    // we can encode for kafka key
    sds kafka_key = sdsnewlen(&dbid, sizeof(dbid));
    uint8_t kafka_cmd_id = KAFKA_COMPACT_HSET_COMMAND;
    kafka_key = sdscatlen(kafka_key, &kafka_cmd_id, sizeof(kafka_cmd_id));
    kafka_key = sdscatlen(kafka_key, key, sdslen(key));
    uint8_t special_char = SPECIAL_CHAR_CAN_NOT_IN_STRING;
    kafka_key = sdscatlen(kafka_key, &special_char, sizeof(special_char));
    kafka_key = sdscatlen(kafka_key, field, sdslen(field));
    
    return kafka_key;
}

static sds cal_kafka_key_for_hmset(uint8_t dbid, list *args) {
    serverAssert(server.kafka_compcation);

    size_t len = listLength(args);
    serverAssert(len % 2 == 1 && len >= 3);
    
    if (len != 3) 
        return NULL;    // only support one key with one field 

    sds key = listNodeValue(listIndex(args, 0));
    sds field = listNodeValue(listIndex(args, 1));

    if (has_special_char_in_key_or_field(key) || has_special_char_in_key_or_field(field))
        return NULL;

    // we can encode for kafka key
    sds kafka_key = sdsnewlen(&dbid, sizeof(dbid));
    uint8_t kafka_cmd_id = KAFKA_COMPACT_HMSET_COMMAND;
    kafka_key = sdscatlen(kafka_key, &kafka_cmd_id, sizeof(kafka_cmd_id));
    kafka_key = sdscatlen(kafka_key, key, sdslen(key));
    uint8_t special_char = SPECIAL_CHAR_CAN_NOT_IN_STRING;
    kafka_key = sdscatlen(kafka_key, &special_char, sizeof(special_char));
    kafka_key = sdscatlen(kafka_key, field, sdslen(field));
    
    return kafka_key;
}

static sds cal_kafka_key_for_hdel(uint8_t dbid, list *args) {
    serverAssert(server.kafka_compcation);

    size_t len = listLength(args);
    serverAssert(len >= 2);
    
    if (len != 2) 
        return NULL;    // only support one key with one field 

    sds key = listNodeValue(listIndex(args, 0));
    sds field = listNodeValue(listIndex(args, 1));

    if (has_special_char_in_key_or_field(key) || has_special_char_in_key_or_field(field))
        return NULL;

    // we can encode for kafka key
    sds kafka_key = sdsnewlen(&dbid, sizeof(dbid));
    uint8_t kafka_cmd_id = KAFKA_COMPACT_HDEL_COMMAND;
    kafka_key = sdscatlen(kafka_key, &kafka_cmd_id, sizeof(kafka_cmd_id));
    kafka_key = sdscatlen(kafka_key, key, sdslen(key));
    uint8_t special_char = SPECIAL_CHAR_CAN_NOT_IN_STRING;
    kafka_key = sdscatlen(kafka_key, &special_char, sizeof(special_char));
    kafka_key = sdscatlen(kafka_key, field, sdslen(field));
    
    return kafka_key;
}

static sds cal_kafka_key_for_del(uint8_t dbid, list *args) {
    serverAssert(server.kafka_compcation);

    size_t len = listLength(args);
    serverAssert(len >= 1);
    
    if (len != 1) 
        return NULL;    // only support one key 

    sds key = listNodeValue(listIndex(args, 0));

    // we can encode for kafka key
    sds kafka_key = sdsnewlen(&dbid, sizeof(dbid));
    uint8_t kafka_cmd_id = KAFKA_COMPACT_DEL_COMMAND;
    kafka_key = sdscatlen(kafka_key, &kafka_cmd_id, sizeof(kafka_cmd_id));
    kafka_key = sdscatlen(kafka_key, key, sdslen(key));
    
    return kafka_key;
}

static sds cal_kafka_key_for_unlink(uint8_t dbid, list *args) {
    serverAssert(server.kafka_compcation);

    size_t len = listLength(args);
    serverAssert(len >= 1);
    
    if (len != 1) 
        return NULL;    // only support one key 

    sds key = listNodeValue(listIndex(args, 0));

    // we can encode for kafka key
    sds kafka_key = sdsnewlen(&dbid, sizeof(dbid));
    uint8_t kafka_cmd_id = KAFKA_COMPACT_UNLINK_COMMAND;
    kafka_key = sdscatlen(kafka_key, &kafka_cmd_id, sizeof(kafka_cmd_id));
    kafka_key = sdscatlen(kafka_key, key, sdslen(key));
    
    return kafka_key;
}

/* when kafka run in compaction for topic, it needs PK, primary key */
static sds cal_kafka_key_for_pk() {
    serverAssert(server.kafka_compcation);

    uint8_t dbid = 255;
    sds kafka_key = sdsnewlen(&dbid, sizeof(dbid));
    uint8_t kafka_cmd_id = KAFKA_COMPACT_PK;
    kafka_key = sdscatlen(kafka_key, &kafka_cmd_id, sizeof(kafka_cmd_id));

    char uuid[37];
    uuid_t binuuid;
    uuid_generate_random(binuuid);
    uuid_unparse(binuuid, uuid);
    kafka_key = sdscatlen(kafka_key, uuid, 37);

    return kafka_key;
}

static sds cal_kafka_key_in_producer_thread(sds msg) {
    if (!server.kafka_compcation) return NULL;

    int is_exec_msg = check_exec_msg(msg);
    
    if (is_exec_msg) {
        return NULL;   // for transaction, no kafka key
    } else {    
        uint8_t node_id;
        uint64_t client_id;
        uint8_t dbid;
        sds command = NULL;
        list *no_exec_args = NULL;

        int parse_ret = parse_msg_for_no_exec(msg, &node_id, &client_id, &dbid, &command, &no_exec_args);
        serverAssert(parse_ret == C_OK);
        serverAssert(command);

        sds kafka_key = NULL;
        if (strcasecmp(command, "set") == 0) {
            kafka_key = cal_kafka_key_for_set(dbid, no_exec_args);
        } else if (strcasecmp(command, "getset") == 0) {
            kafka_key = cal_kafka_key_for_getset(dbid, no_exec_args);
        } else if (strcasecmp(command, "getset") == 0) {
            kafka_key = cal_kafka_key_for_getset(dbid, no_exec_args);
        } else if (strcasecmp(command, "getdel") == 0) {
            kafka_key = cal_kafka_key_for_getdel(dbid, no_exec_args);
        } else if (strcasecmp(command, "hset") == 0) {
            kafka_key = cal_kafka_key_for_hset(dbid, no_exec_args);
        } else if (strcasecmp(command, "hmset") == 0) {
            kafka_key = cal_kafka_key_for_hmset(dbid, no_exec_args);
        } else if (strcasecmp(command, "hdel") == 0) {
            kafka_key = cal_kafka_key_for_hdel(dbid, no_exec_args);
        } else if (strcasecmp(command, "del") == 0) {
            kafka_key = cal_kafka_key_for_del(dbid, no_exec_args);
        } else if (strcasecmp(command, "unlink") == 0) {
            kafka_key = cal_kafka_key_for_unlink(dbid, no_exec_args);
        }

        if (!kafka_key) kafka_key = cal_kafka_key_for_pk();

        // recycle the resource allocated by parse_msg_for_no_exec()
        sdsfree(command);
        if (no_exec_args) {
            listSetFreeMethod(no_exec_args, freeArgAsSdsString);
            listRelease(no_exec_args);
        }

        serverAssert(kafka_key);
        return kafka_key;
    }
}

/* reference https://github.com/edenhill/librdkafka/blob/master/examples/idempotent_producer.c */
static void sendKafkaMsgInProducerThread(sds msg, rd_kafka_t *rk, rd_kafka_topic_t *rkt) {
    serverAssert(msg);

    sds kafka_key = cal_kafka_key_in_producer_thread(msg);

    int errno;
    while (1) {
        // NOTE: msg_opaque parameter needs to be msg to reclame the memory in db_msg_cb()
        errno = rd_kafka_produce(rkt, 0, 0, msg, sdslen(msg), 
                                 kafka_key, kafka_key ? sdslen(kafka_key) : 0, msg);    

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
            if (strcmp(rd_kafka_err2name(rd_kafka_last_error()), "MSG_SIZE_TOO_LARGE") == 0)
                serverLog(LL_WARNING, "producer found MSG_SIZE_TOO_LARGE, msg size = %ld", sdslen(msg));
            serverPanic("sendKafkaMsgInProducerThread() fatel error, errono = %d, reason = %s",
                        errno, rd_kafka_err2name(rd_kafka_last_error()));
        }
    }

    sdsfree(kafka_key);

    serverPanic( "sendKafkaMsgInProducerThread() can not reach here!");
}

/* We use rd_kafka_metadata() to check the topic valid in Kafka 
 * Most rdkafka API are local only, we need a API for test Kafka liveness 
 * if no error, return C_OK. otherwise, return C_ERR */
#define CLEANUP_POLICY_MAX_SIZE 16
static int checkKafkaInProduerThread(rd_kafka_t *rk, rd_kafka_topic_t *rkt) {
    const struct rd_kafka_metadata *data;
    rd_kafka_resp_err_t err = rd_kafka_metadata(rk, 0, rkt, &data, 5000);  // 5 seconds
    if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        rd_kafka_metadata_destroy(data);
        return C_OK;
    } else {
        serverLog(LL_WARNING, "checkKafkaInProduerThread() failed for reason = %s", rd_kafka_err2str(err));
        return C_ERR;
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

    serverAssert(bootstrapBrokers);

    conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstrapBrokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer failed for rd_kafka_conf_set() bootstrap.servers, reason = %s", errstr);
    if (rd_kafka_conf_set(conf, "linger.ms", "0",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer failed for rd_kafka_conf_set() linger.ms, reason = %s", errstr);
    // set time out to be infinite
    if (rd_kafka_conf_set(conf, "message.timeout.ms", "0",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer failed for rd_kafka_conf_set() message.timeout.ms, reason = %s", errstr);
    if (rd_kafka_conf_set(conf, "enable.idempotence", "true",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer failed for rd_kafka_conf_set() enable.idempotence, reason = %s", errstr);
    // message.max.bytes
    sds message_max_bytess = sdsfromlonglong(2 * MARSHALL_SIZE_OVERFLOW);
    if (rd_kafka_conf_set(conf, "message.max.bytes", message_max_bytess,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer failed for rd_kafka_conf_set() message.max.bytes, reason = %s", errstr);
    sdsfree(message_max_bytess);
    // set producer compression type of LZ4
    if (rd_kafka_conf_set(conf, "compression.type", "lz4",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaProducer failed for rd_kafka_conf_set() compression.type, reason = %s", errstr);

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

    if (checkKafkaInProduerThread(rk, rkt) != C_OK) {
        serverLog(LL_WARNING, "producer thread can not connect to Kafka, so terminate the thread!");
        return NULL;    // terminate the producer thread if can not connect to Kafka
    } else {
        kafkaReadyAndTopicCorrect = 1;  // main thread check the value here
    }

    set_compression_type_for_topic();

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
    bootstrapBrokers = get_kafka_broker();

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
        usleep(1001*1000);
        ++seconds;
        serverLog(LL_NOTICE, "Main thread waiting for producer thread reporting kafka state ...");
    }

    lockProducerData();
    if (kafkaReadyAndTopicCorrect != 1) {
        serverLog(LL_WARNING, "Main thread check kafka state result failed for %d seconds!", seconds);
        unlockProducerData();
        exit(1);
    } 
    unlockProducerData();

    // init Kafka producer thread succesfully. We can go on.
}    

/*                                 */
/*                                 */
/* The following is about consumer */
/*                                 */
/*                                 */

static void resume_process_cmd_by_concrete(client *c) {
    int res = processCommandAndResetClient(c);
    serverAssert(res != C_ERR);
    serverAssert(!(c->flags & CLIENT_MULTI));

    if (c->querybuf && sdslen(c->querybuf) > 0) 
        processInputBuffer(c);
}

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

    // for debug
    if (server._debug_) {
        if (strcasecmp(command, "append") == 0) {
            sds key = listNodeValue(listIndex(args, 0));
            int ret = debug_set_string_key_rock(dbid, key);
            if (ret == 0)
                serverLog(LL_WARNING, "debug: evict string key %s to rock", key);
        }
    }

    if (c != server.virtual_client) {
        serverAssert(c->streamWriting == STREAM_WRITE_WAITING);
        serverAssert(strcasecmp(c->argv[0]->ptr, command) == 0);
        serverAssert((size_t)c->argc == 1 + (args ? listLength(args) : 0));

        c->streamWriting = STREAM_WRITE_FINISH;

        checkAndSetRockKeyNumber(c, 1);        // after the stream phase, we can goon to rock phase
        if (c->rockKeyNumber == 0)
            // resume the excecution of concrete client and clear server.streamCurrentClient
            resume_process_cmd_by_concrete(c);        

    } else {
        setVirtualClientContextForNoTransaction(dbid, command, args);

        checkAndSetRockKeyNumber(c, 1);        // after the stream phase, we can goon to rock phase 
        if (c->rockKeyNumber == 0) 
            // resume the execution of virtual client and clear server.streamCurrentClient
            execVirtualCommand();              
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
    serverAssert(listLength(cmds) == listLength(args));

    client *c = server.virtual_client;
    if (node_id == server.node_id) {
        dictEntry *de = dictFind(server.clientIdTable, (const void*)client_id);
        if (de) c = dictGetVal(de);     // found the concrete client
    }
    server.streamCurrentClientId = c->id;   // NOTE: could be concrete client id or vritual client id

    // for debug
    if (server._debug_) {
        listIter li;
        listNode *ln;
        listRewind(cmds, &li);
        int index = 0;
        while ((ln = listNext(&li))) {
            sds cmd = listNodeValue(ln);
            if (strcasecmp(cmd, "append") == 0) {
                list *append_args = listNodeValue(listIndex(args, index));
                serverAssert(append_args);
                sds key = listNodeValue(listIndex(append_args, 0));
                int ret = debug_set_string_key_rock(dbid, key);
                if (ret == 0)
                    serverLog(LL_WARNING, "debug: evict string key %s to rock", key);
            }
            ++index;
        }
    }

    if (c != server.virtual_client) {
        serverAssert(c->streamWriting == STREAM_WRITE_WAITING);
        serverAssert(strcasecmp(c->argv[0]->ptr, "exec") == 0);
        serverAssert(c->argc == 1);

        c->streamWriting = STREAM_WRITE_FINISH;

        checkAndSetRockKeyNumber(c, 1);        // after the stream phase, we can goon to rock phase
        if (c->rockKeyNumber == 0) {
            // resume the excecution of concrete client and clear server.streamCurrentClient

            // There is a bug if call processCommandAndResetClient() directly by concrete client
            // when this or other connection drop or establish. Unnkonw reason
            resume_process_cmd_by_concrete(c);
        }        

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
    listRewind(args, &li);
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
        if (parse_ret != C_OK) 
            serverPanic( "fatel error for parse message!");
 
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
    if (listLength(rcvMsgs) >= RCV_MSGS_TOO_LONG) {
        int consume_startup;
        atomicGet(kafkaStartupConsumeFinish, consume_startup);
        if (consume_startup == CONSUMER_STARTUP_OPEN_TO_CLIENTS)
            serverLog(LL_WARNING, "addRcvMsgInConsumerThread() rcvMsgs too long, list length = %ld", 
                      listLength(rcvMsgs));
    }
        
    unlockConsumerData();
}

/*  retrieve current offset range [lo, hi), If topic never created, it will fail and exit 
 *  guararntee:
 *             1. lo <= hi
 *             2. lo <= hi and lo >= 0 */
static void get_offset_range(rd_kafka_t *rk, int64_t *lo, int64_t *hi) {
    int max_try_times = 5;
    rd_kafka_resp_err_t err;
    for (int i = 0; i < max_try_times; ++i) {
        err = rd_kafka_query_watermark_offsets(rk, bunnyRedisTopic, 0, lo, hi, 2000);
        if (err == RD_KAFKA_RESP_ERR_NO_ERROR) break;
        usleep(1000*1000);   // sleep for 1 second
    }
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        serverLog(LL_WARNING, "get_offset_range() failed, err = %d, err desc = %s.", 
                  err, rd_kafka_err2str(err));
        exit(1);
    }
    serverAssert(*lo >= 0 && *lo <= *hi);
}

/* this is the consumer thread entrance */
static void* entryInConsumerThread(void *arg) {
    UNUSED(arg);

    rd_kafka_t *rk;          /* Consumer instance handle */
    rd_kafka_conf_t *conf;   /* Temporary configuration object */
    rd_kafka_resp_err_t err; /* librdkafka API error code */
    char errstr[512];        /* librdkafka API error reporting buffer */
    rd_kafka_topic_partition_list_t *partition_list; /* Subscribed topics */
    rd_kafka_topic_partition_t *zero_partition;

    rcvMsgs = listCreate();

    // generate uuid for groupid, so ervery consumer is a single group in kafka
    char uuid[37];
    uuid_t binuuid;
    uuid_generate_random(binuuid);
    uuid_unparse(binuuid, uuid);

    serverAssert(bootstrapBrokers);

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
    if (rd_kafka_conf_set(conf, "enable.auto.commit", "false", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaConsumer failed for rd_kafka_conf_set() enable.auto.commit, reason = %s", errstr);
    sds val_fetch_message_max_bytes = sdsfromlonglong(MARSHALL_SIZE_OVERFLOW*2);
    sds val_recieve_message_max_bytes = sdsfromlonglong(MARSHALL_SIZE_OVERFLOW*2+512);
    if (rd_kafka_conf_set(conf, "fetch.message.max.bytes", val_fetch_message_max_bytes, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaConsumer failed for rd_kafka_conf_set() fetch.message.max.bytes, reason = %s", errstr);
    if (rd_kafka_conf_set(conf, "fetch.max.bytes", val_fetch_message_max_bytes, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaConsumer failed for rd_kafka_conf_set() fetch.max.bytes, reason = %s", errstr);
    if (rd_kafka_conf_set(conf, "receive.message.max.bytes", val_recieve_message_max_bytes, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        serverPanic("initKafkaConsumer failed for rd_kafka_conf_set() receive.message.max.bytes, reason = %s", errstr);
    sdsfree(val_fetch_message_max_bytes);
    sdsfree(val_recieve_message_max_bytes);
    
    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk)
        serverPanic("initKafkaConsumer failed for rd_kafka_new(), errstr = %s", errstr);
    conf = NULL; /* Configuration object is now owned, and freed, by the rd_kafka_t instance. */
    rd_kafka_poll_set_consumer(rk);
 
    partition_list = rd_kafka_topic_partition_list_new(1);
    zero_partition = rd_kafka_topic_partition_list_add(partition_list, bunnyRedisTopic, 0);
    /* Subscribe to the list of topics, NOTE: only one in the list of topics */
    zero_partition->offset = RD_KAFKA_OFFSET_BEGINNING;
    err = rd_kafka_assign(rk, partition_list);
    if (err) 
        serverPanic("initKafkaConsumer failed for rd_kafka_subscribe() reason = %s", rd_kafka_err2str(err));
        
    int64_t lo_offset, hi_offset;
    get_offset_range(rk, &lo_offset, &hi_offset); 
    int64_t total_resume_cnt = hi_offset-lo_offset;
    serverLog(LL_NOTICE, "Kafka log offset range is [%ld, %ld), total need resume = %ld", 
              lo_offset, hi_offset, total_resume_cnt);
    check_or_set_offset(lo_offset);

    rd_kafka_topic_partition_list_destroy(partition_list);

    // then set broker max.message.bytes
    set_max_message_bytes(2 * MARSHALL_SIZE_OVERFLOW);

    // loop for Kafka messages
    monotime startupCheckTimer = 0;
    int is_first_msg_arrived = 0;
    if (lo_offset == hi_offset) {
        atomicSet(kafkaStartupConsumeFinish, CONSUMER_STARTUP_FINISH);
        is_first_msg_arrived = 1;
        serverLog(LL_NOTICE, "consumer startup does not need to recover Kafka old messages.");
    } else {
        elapsedStart(&startupCheckTimer);
    }
    while(1) {
        rd_kafka_message_t *rkm = rd_kafka_consumer_poll(rk, 100);

        if (!rkm) 
            continue;     // timeout with no message 
        
        if (rkm->err) {
            if (rkm->err == RD_KAFKA_RESP_ERR__AUTO_OFFSET_RESET) {
                serverLog(LL_WARNING, "offset for Broker: Offset out of range!!! rkm->offset = %ld", rkm->offset);
                exit(1);
            }
            serverLog(LL_WARNING, "Consumer error but Kafka can handle: err = %d, reason = %s", 
                      rkm->err, rd_kafka_message_errstr(rkm));
            rd_kafka_message_destroy(rkm);
            continue;
        }

        // proper message. The following process the message and set up startup state

        // check whether first message offset is matched
        int64_t cur_offset = rkm->offset;
        serverAssert(cur_offset >= 0);
        if (!is_first_msg_arrived) {
            is_first_msg_arrived = 1;
            if (cur_offset != lo_offset) {
                serverLog(LL_WARNING, "the first Kafka message's offset not match, first offset = %ld, lo_offset = %ld",
                          cur_offset, lo_offset);
                exit(1);
            }
        } 

        serverAssert(strcmp(rd_kafka_topic_name(rkm->rkt), bunnyRedisTopic) == 0 && rkm->len > 0);
        addRcvMsgInConsumerThread(rkm->len, rkm->payload);
        // notify main thread to perform streamConsumerSignalHandler()
        signalMainThreadByPipeInConsumerThread();   
    
        rd_kafka_message_destroy(rkm);

        // check whether the consumer startup is finished
        if (cur_offset >= hi_offset - 1) {
            int consumer_startup;
            atomicGet(kafkaStartupConsumeFinish, consumer_startup);
            if (consumer_startup == CONSUMER_STARTUP_START) {
                atomicSet(kafkaStartupConsumeFinish, CONSUMER_STARTUP_FINISH);
                serverLog(LL_NOTICE, "consumer startup finished recovering Kafka old messages.");
            }
        } else {
            // report progress of startup
            static int consumer_wait_cnt = 0;
            if (zmalloc_used_memory() > server.bunnymem) {
                if (consumer_wait_cnt == 0)
                    serverLog(LL_NOTICE, "memory over limit while startup, waiting for background job to evict some memeory");
                while (zmalloc_used_memory() > server.bunnymem) {     
                    usleep(5*1000);
                }
                if (consumer_wait_cnt == 0)
                    serverLog(LL_NOTICE, "startup can contnue with some memory evicted to RocksDB...");
                ++consumer_wait_cnt;
                consumer_wait_cnt %= 128;
            }
            int64_t processed_cnt = cur_offset - lo_offset + 1;
            int percentage = (int)(processed_cnt * 100 / total_resume_cnt);
            if (processed_cnt % 50000 == 0) {
                serverLog(LL_NOTICE, "consumer thread is resuming Kafka log, processed count = %ld(%d%%), latency = %lu(ms)",
                          processed_cnt, percentage, elapsedMs(startupCheckTimer));
                elapsedStart(&startupCheckTimer);
            }
        }
    }

    return NULL;
}

/* the function create a stream reader thread, which will read data from kafka 
 * when stream reader thread read some write operations from kafka,
 * it will write it to buffer (sync by spin lock) then notify main thread by pipe 
 * because main thread maybe sleep in eventloop for events comming */
void initStreamPipeAndStartConsumer() {
    // first, we need to pause all clients for consumer thread to do some startup works
    atomicSet(kafkaStartupConsumeFinish, CONSUMER_STARTUP_START);

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

    if (pthread_create(&consumer_thread, NULL, entryInConsumerThread, NULL) != 0) 
        serverPanic("Unable to create a consumer thread.");
}

/* NOTE: thread safe */
int is_startup_on_going() {
    int consumer_startup;
    atomicGet(kafkaStartupConsumeFinish, consumer_startup);
    return consumer_startup == CONSUMER_STARTUP_START;
}