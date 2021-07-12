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

#define THREADED 1
#define _GNU_SOURCE     /* To get defns of NI_MAXSERV and NI_MAXHOST */

#include "server.h"

#include "zookeeper/zookeeper.h"
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <linux/if_link.h>
#include <jansson.h>


#define BUNNY_ZK_ROOT_NODE "/bunnyredis"
static zhandle_t *zh;
static char buffer[512];
// static int buf_len;
static struct Stat stat;

static void byte_to_hex_chars(uint8_t v, char *buf) {
    const char * hex = "0123456789ABCDEF";
    buf[0] = hex[(v>>4)&0x0F];
    buf[1] = hex[v&0x0F];
}

static uint8_t get_available_node_id() {    
    // NOTE: available node id can not be 255
    for (int i = 0; i < 255; ++i) {
        sds nodeid_znode = sdsnew(BUNNY_ZK_ROOT_NODE);
        nodeid_znode = sdscatlen(nodeid_znode, "/", 1);
        uint8_t node_id = (uint8_t)i;
        char hex[2];
        byte_to_hex_chars(node_id, hex);
        nodeid_znode = sdscatlen(nodeid_znode, hex, 2);

        int buf_len = sizeof(buffer);
        int rc = zoo_get(zh, nodeid_znode, 0, buffer, &buf_len, &stat);        
        if (rc != ZOK) {
            if (rc != ZNONODE) {
                serverLog(LL_WARNING, "get_available_node_id() failed for nodeid_znode = %s, rc = %d", 
                          nodeid_znode, rc);
                exit(1);
            }
            // We found one available node id
            sdsfree(nodeid_znode);
            return node_id;
        }

        sdsfree(nodeid_znode);
    }

    serverLog(LL_WARNING, "get_available_node_id() failed for no room!");
    exit(1);
}

static void create_znode_for_node_id(uint8_t node_id, const char* my_ip_port, const char* zk_server) {
    sds nodeid_znode = sdsnew(BUNNY_ZK_ROOT_NODE);
    nodeid_znode = sdscatlen(nodeid_znode, "/", 1);
    char hex[2];
    byte_to_hex_chars(node_id, hex);
    nodeid_znode = sdscatlen(nodeid_znode, hex, 2);

    int rc = zoo_create(zh, nodeid_znode, my_ip_port, strlen(my_ip_port), 
                        &ZOO_OPEN_ACL_UNSAFE, ZOO_PERSISTENT, buffer, sizeof(buffer)-1);
    if (rc != ZOK) {
        serverLog(LL_WARNING, "create_znode_for_node_id() failed, node_id = %d, my_ip_port = %s, zk_server = %s, rc = %d",
                  node_id, my_ip_port, zk_server, rc);
        exit(1);
    }
    sdsfree(nodeid_znode);
}

static void create_znode_for_ip_port(uint8_t node_id, const char* my_ip_port, const char* zk_server) {
    sds my_znode = sdsnew(BUNNY_ZK_ROOT_NODE);
    my_znode = sdscatlen(my_znode, "/", 1);
    my_znode = sdscatlen(my_znode, my_ip_port, strlen(my_ip_port));
    char my_node_val[1];
    my_node_val[0] = node_id;
    int rc = zoo_create(zh, my_znode, my_node_val, 1, 
                        &ZOO_OPEN_ACL_UNSAFE, ZOO_PERSISTENT, buffer, sizeof(buffer)-1);
    if (rc != ZOK) {
        serverLog(LL_WARNING, "create_znode_for_ip_port() failed, node_id = %d, my_ip_port = %s, zk_server = %s, rc = %d",
                  node_id, my_ip_port, zk_server, rc);
        exit(1);
    }
    sdsfree(my_znode);
}

/* if create BunnyReids root znode, return 1; else return 0 */
static int check_or_create_root_znode(const char* zk_server) {
    sds root_znode = sdsnew(BUNNY_ZK_ROOT_NODE);
    int buf_len = sizeof(buffer);
    int rc = zoo_get(zh, root_znode, 0, buffer, &buf_len, &stat);
    if (rc != ZOK) {
        if (rc != ZNONODE) {
            serverLog(LL_WARNING, "check_or_set_root_znode() zoo_get failed for %s, zk_server = %s, rc = %d", 
                      root_znode, zk_server, rc);
            exit(1);
        }
        // create root node
        char *root_value = "This is the BunnyRedis root znode!";
        rc = zoo_create(zh, root_znode, root_value, strlen(root_value), 
                        &ZOO_OPEN_ACL_UNSAFE, ZOO_PERSISTENT, buffer, sizeof(buffer)-1);
        if (rc != ZOK) {
            serverLog(LL_WARNING, "check_or_set_root_znode() zoo_create failed for %s, zk_server = %s, rc = %d", 
                      root_znode, zk_server, rc);
            exit(1);
        }
        serverLog(LL_NOTICE, "create zk root node for zookeeper for %s", root_znode);
        sdsfree(root_znode);
        return 1;
    } else {
        sdsfree(root_znode);
        return 0;
    }
}

static void verify_found_node_id(uint8_t node_id, char *my_ip_port) {
    sds check_znode = sdsnew(BUNNY_ZK_ROOT_NODE);
    check_znode = sdscatlen(check_znode, "/", 1);
    char hex[2];
    byte_to_hex_chars(node_id, hex);
    check_znode = sdscatlen(check_znode, hex, 2);
    
    int buf_len = sizeof(buffer);
    int rc = zoo_get(zh, check_znode, 0, buffer, &buf_len, &stat);
    if (rc != ZOK) {
        serverLog(LL_WARNING, "verify_found_node_id() zoo_get failed, node_id = %d, ip_port = %s, rc = %d", 
                  node_id, my_ip_port, rc);
        exit(1);
    }

    if (buf_len != (int)strlen(my_ip_port)) {
        serverLog(LL_WARNING, "verify_found_node_id() buf_len != strlen(ip_port) failed, node_id = %d, ip_port = %s, buf_len = %d", 
                  node_id, my_ip_port, buf_len);
        exit(1);
    }

    if (memcmp(buffer, my_ip_port, buf_len) != 0) {
        sds real_read = sdsnewlen(buffer, buf_len);
        serverLog(LL_WARNING, "verify_found_node_id() memcmp failed, node_id = %d, ip_port = %s, buffer = %s", 
                  node_id, my_ip_port, real_read);
        exit(1);
    }
}

// refererence https://man7.org/linux/man-pages/man3/getifaddrs.3.html
static void get_ip_addresss(char *ip, size_t ip_max_len) {
    serverAssert(ip_max_len);
    ip[0] = 0;

    struct ifaddrs *ifaddr;
    int family, s;
    char host[NI_MAXHOST];

    if (getifaddrs(&ifaddr) == -1) {
        serverLog(LL_WARNING, "getifaddrs failed!");
        exit(1);
    }

    for (struct ifaddrs *ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)  
            continue;

        if (strcmp(ifa->ifa_name, "lo") == 0) 
            continue;       // we do not need "127.0.0.1"

        family = ifa->ifa_addr->sa_family;
        if (family == AF_INET) {        
            // only support IP4
            s = getnameinfo(ifa->ifa_addr,
                            sizeof(struct sockaddr_in),
                            host, NI_MAXHOST,
                            NULL, 0, NI_NUMERICHOST);
            if (s != 0) {
                serverLog(LL_WARNING, "getnameinfo() failed: %s\n", gai_strerror(s));
                exit(1);
            }

            if (strlen(host) <= ip_max_len) {
                memcpy(ip, host, strlen(host)+1);
                break;
            }
        }
    }

    freeifaddrs(ifaddr);

    if (ip[0] == 0) {
        serverLog(LL_WARNING, "Can not find a valid IP4 ip address. NOTE: ip excludes 127.0.0.1 local address.");
        exit(1);
    }
}


static int is_compact_cleanup_policy() {
    char *stream_write_znode = "/config/topics/redisStreamWrite";
    int buf_len = sizeof(buffer);
    int rc = zoo_get(zh, stream_write_znode, 0, buffer, &buf_len, &stat);
    if (rc) {
        if (rc != ZNONODE) {
            serverLog(LL_WARNING, "is_compact_cleanup_policy() failed, rc = %d", rc);
            exit(1);
        }
        return 0;       // not found, default cleanup policy is "delete", so return false
    } 
    
    json_error_t err;
    json_t *root = json_loadb(buffer, buf_len, 0, &err);
    if (!root || json_typeof(root) != JSON_OBJECT) {
        serverLog(LL_WARNING, "is_compact_cleanup_policy() root not Json or root not an object Json!");
        exit(1);
    }
    json_t *config = json_object_get(root, "config");
    if (!config || json_typeof(config) != JSON_OBJECT) {
        serverLog(LL_WARNING, "is_compact_cleanup_policy() config not get or config not an object Json");
        exit(1);
    }

    json_t *cleanup_policy = json_object_get(config, "cleanup.policy");
    if (!cleanup_policy) {
        // not found, it is OK, default is delete policy
        return 0;
    }
    if (json_typeof(cleanup_policy) != JSON_STRING) {
        serverLog(LL_WARNING, "is_compact_cleanup_policy() cleanup.policy is not a string Json");
        exit(1);
    }

    const char *policy_name = json_string_value(cleanup_policy);
    if (strcasecmp(policy_name, "delete") == 0) {
        json_decref(root);
        return 0;
    } else if (strcasecmp(policy_name, "compact") == 0) {
        json_decref(root);
        return 0;
    } else {
        serverLog(LL_WARNING, "cleanup.policy is wrong with value = %s! either delete or compact. We forbid delete and compact",
                  policy_name);
        exit(1);
    }
}


# define MAX_IP4_LEN    15  // max ip4 is 255.255.255.255    
uint8_t init_zk_and_get_node_id() {
    // NOTE: we use different free or malloc
    json_set_alloc_funcs(zmalloc, zfree);

    char ip[MAX_IP4_LEN+1];
    get_ip_addresss(ip, MAX_IP4_LEN);
    serverLog(LL_NOTICE, "ip addresss = %s", ip);
    char port[32];
    sprintf(port, "%d", server.port);
    sds my_ip_port = sdsnew(ip);
    my_ip_port = sdscatlen(my_ip_port, ":", 1);
    my_ip_port = sdscatlen(my_ip_port, port, strlen(port));

    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);

    zh = zookeeper_init(server.zk_server, NULL, 10000, NULL, NULL, 0);
    if (!zh) {
        serverLog(LL_WARNING, "zookeeper_init() failed, zk_server = %s, errno = %d\n", server.zk_server, errno);
        exit(1);
    } 

    // check whether Kafka in compaction mode
    if (is_compact_cleanup_policy()) {
        serverLog(LL_NOTICE, "Kafka is in compact mode. So BunnyRedis will be in compaction mode.");
        server.kafka_compcation = 1;
    } else {
        server.kafka_compcation = 0;
    }

    // check root
    check_or_create_root_znode(server.zk_server);

    // char *my_ip_port = "127.0.0.1:6379";
    sds my_znode = sdsnew(BUNNY_ZK_ROOT_NODE);
    my_znode = sdscatlen(my_znode, "/", 1);
    my_znode = sdscatlen(my_znode, my_ip_port, strlen(my_ip_port));
    int buf_len = sizeof(buffer);
    int rc = zoo_get(zh, my_znode, 0, buffer, &buf_len, &stat);
    if (rc != ZOK) {
        if (rc != ZNONODE) {
            serverLog(LL_WARNING, "get zk my node failed for %s, zk_server = %s, rc = %d", 
                      my_znode, server.zk_server, rc);
            exit(1);
        }
        // try to find an available node id
        uint8_t available_id = get_available_node_id();

        // create two znodes for my_zonde and node_id
        create_znode_for_ip_port(available_id, my_ip_port, server.zk_server);
        create_znode_for_node_id(available_id, my_ip_port, server.zk_server);

        sdsfree(my_znode);
        sdsfree(my_ip_port);
        zookeeper_close(zh);
        return available_id;

    } else {
        // we found my node, return the node id with it
        if (buf_len != 1) {
            serverLog(LL_WARNING, "get zk my node failed, bu_len != 1,  buf_len = %d, for %s, zk_server = %s, rc = %d", 
                      buf_len, my_znode, server.zk_server, rc);
            exit(1);
        }
        
        uint8_t node_id = buffer[0];

        // we need check reversely by lookup the znode of node_id and verify my_ip_port
        verify_found_node_id(node_id, my_ip_port);

        sdsfree(my_znode);
        sdsfree(my_ip_port);
        zookeeper_close(zh);
        return node_id;
    }
}

/* This is for stream write consumer thread startup check 
 * In delete cleanup.policy, when BunnyRedis start, it will check 
 * the offset in Kafka is the same. If not same, it means 
 * some log are missing. When missing, The server can not go on 
 * NOTE: offset could be -1. it means no messegs in topic right now
 *       otherwise, it is greater or equal to zero, and it means 
 *                  the first message offset be consumed by whole cluster */
void check_or_set_offset(int64_t offset) {
    serverAssert(offset >= 0);

    if (server.kafka_compcation) return;        // No need to check and set in kafka_compcation mode

    zh = zookeeper_init(server.zk_server, NULL, 10000, NULL, NULL, 0);
    if (!zh) {
        serverLog(LL_WARNING, "check_or_set_offset() failed, zk_server = %s, errno = %d\n", 
                  server.zk_server, errno);
        exit(1);
    } 

    sds offset_znode = sdsnew(BUNNY_ZK_ROOT_NODE);
    char *sub_path = "/offset";
    offset_znode = sdscatlen(offset_znode, sub_path, strlen(sub_path));

    int buf_len = sizeof(buffer);
    int rc = zoo_get(zh, offset_znode, 0, buffer, &buf_len, &stat);
    if (rc != ZOK) {
        if (rc != ZNONODE) {
            serverLog(LL_WARNING, "check_or_set_offset() failed for zoo_get, znode = %s, zk_server = %s, rc = %d", 
                      offset_znode, server.zk_server, rc);
            exit(1);
        }
        // not found, create one
        int64_t kafka_offset = intrev64ifbe(offset);
        rc = zoo_create(zh, offset_znode, (char*)&kafka_offset, sizeof(kafka_offset), 
                        &ZOO_OPEN_ACL_UNSAFE, ZOO_PERSISTENT, buffer, sizeof(buffer)-1);
        if (rc != ZOK) {
            serverLog(LL_WARNING, "check_or_set_offset() failed for zoo_create, znode = %s, zk_server = %s, rc = %d", 
                      offset_znode, server.zk_server, rc);
            exit(1);
        }
    } else {
        // found some node write the consumed offset before, then check whether they are same
        if (buf_len != sizeof(offset)) {
            serverLog(LL_WARNING, "check_or_set_offset() failed for buf_len != sizeof(offset), znode = %s, zk_server = %s, buf_len = %d", 
                      offset_znode, server.zk_server, buf_len);
            exit(1);
        }
        unsigned char *p = (unsigned char *)buffer;
        uint64_t saved_offset = intrev64ifbe(*(uint64_t*)p);
        if (offset != (int64_t)saved_offset) {
            serverLog(LL_WARNING, "check_or_set_offset() failed for not same offset, znode = %s, zk_server = %s, offset = %ld, saved_offset = %ld", 
                      offset_znode, server.zk_server, offset, saved_offset);
            exit(1);
        }
    }

    sdsfree(offset_znode);
    zookeeper_close(zh);
}

/* use cli_mt to get /brokers/ids/0 for the data format in read */
static sds parse_endpoint(sds read) {
    json_error_t err;
    json_t *root = json_loads(read, 0, &err);
    if (!root || json_typeof(root) != JSON_OBJECT) {
        serverLog(LL_WARNING, "parse_endpoint() load read as json failed! read = %s", read);
        exit(1);
    }

    json_t *host = json_object_get(root, "host");
    if (!host || json_typeof(host) != JSON_STRING) {
        serverLog(LL_WARNING, "parse_endpoint() host failed! read = %s", read);
        exit(1);
    }
    const char *host_str = json_string_value(host);

    json_t *port = json_object_get(root, "port");
    if (!port || json_typeof(port) != JSON_INTEGER) {
        serverLog(LL_WARNING, "parse_endpoint() port failed! read = %s", read);
        exit(1);
    }
    json_int_t port_val = json_integer_value(port);
    
    sds res = sdsnewlen(host_str, strlen(host_str));
    res = sdscatlen(res, ":", 1);
    sds port_str = sdsfromlonglong((long long)port_val);
    res = sdscatlen(res, port_str, strlen(port_str));
    sdsfree(port_str);
    
    json_decref(root);
    return res;
}

/*
sds get_kafka_broker() {
    zh = zookeeper_init(server.zk_server, NULL, 10000, NULL, NULL, 0);
    if (!zh) {
        serverLog(LL_WARNING, "get_kafka_broker() failed, zk_server = %s, errno = %d\n", 
                  server.zk_server, errno);
        exit(1);
    } 

    char *ids_path = "/brokers/ids";
    for (size_t i = 0; i < 10; ++i) {
        sds id_znode = sdsnew(ids_path);
        id_znode = sdscatlen(id_znode, "/", 1);
        char ch = '0' + i;
        id_znode = sdscatlen(id_znode, &ch, 1);

        int buf_len = sizeof(buffer);
        int rc = zoo_get(zh, id_znode, 0, buffer, &buf_len, &stat);

        if (rc) {
            sdsfree(id_znode);
            continue;
        }

        if (buf_len < 0) {
            serverLog(LL_WARNING, "no data in id_znode = %s", id_znode);
            sdsfree(id_znode);
            continue;
        }

        sds read_json_str = sdsnewlen(buffer, (size_t)buf_len);
        sds endpoint = parse_endpoint(read_json_str);
        sdsfree(read_json_str);
        zookeeper_close(zh);
        serverLog(LL_WARNING, "Kafka broker endpoint = %s", endpoint);
        return endpoint;
    }

    serverLog(LL_WARNING, "get_kafka_broker() can not get Kafka broker");
    exit(1);
}
*/

sds get_kafka_brokers() {
    zh = zookeeper_init(server.zk_server, NULL, 10000, NULL, NULL, 0);
    if (!zh) {
        serverLog(LL_WARNING, "get_kafka_brokers() failed, zk_server = %s, errno = %d\n", 
                  server.zk_server, errno);
        exit(1);
    } 

    int rc;
    struct String_vector ids;
    char *ids_znode = "/brokers/ids";
    rc = zoo_get_children(zh, ids_znode, 0, &ids);
    if (rc != ZOK) {
        serverLog(LL_WARNING, "zoo_get_children() failed for rc = %d", rc);
        exit(1);
    }

    if (ids.count <= 0) {
        serverLog(LL_WARNING, "No Kafka broker ids found in Zookeeper");
        exit(1);
    }

    sds brokers = sdsnew(NULL);
    for (int32_t i = 0; i < ids.count; ++i) {
        sds one_broker_znode = sdsnew(ids_znode);
        one_broker_znode = sdscatlen(one_broker_znode, "/", 1);
        one_broker_znode = sdscatlen(one_broker_znode, ids.data[i], strlen(ids.data[i]));
        int buf_len = sizeof(buffer);
        rc = zoo_get(zh, one_broker_znode, 0, buffer, &buf_len, &stat);
        if (rc || buf_len < 0) {
            serverLog(LL_WARNING, "get_kafka_brokers() failed for zoo_get() for one_broker_znode = %s", one_broker_znode);
            exit(1);
        }
        sds read_json_str = sdsnewlen(buffer, (size_t)buf_len);
        sds endpoint = parse_endpoint(read_json_str);

        if (i != 0) {
            brokers = sdscatlen(brokers, ",", 1);
        } 
        brokers = sdscatlen(brokers, endpoint, sdslen(endpoint));

        sdsfree(read_json_str);
        sdsfree(endpoint);
        sdsfree(one_broker_znode);
    }

    zookeeper_close(zh);
    return brokers;
}

static void create_change_znode(zhandle_t *zh) {
    int rc;

    char *changes_znode = "/config/changes";
    struct String_vector changes;
    rc = zoo_get_children(zh, changes_znode, 0, &changes);
    if (rc != ZOK) {
        serverLog(LL_WARNING, "create_change_znode() failed for rc = %d", rc);
        exit(1);
    }

    char *new_znode = "/config/changes/config_change_";
    // sds new_change_znode = sdsnewlen(prefix, strlen(prefix));
    // new_change_znode = sdscatlen(new_change_znode, digits, 10);
    char *new_node_val = "{\"version\":2,\"entity_path\":\"topics/redisStreamWrite\"}";
    rc = zoo_create(zh, new_znode, new_node_val, strlen(new_node_val), 
                    &ZOO_OPEN_ACL_UNSAFE, ZOO_PERSISTENT_SEQUENTIAL, buffer, sizeof(buffer)-1);
    if (rc != ZOK) {
        serverLog(LL_WARNING, "create_change_znode() failed for zoo_create! rc = %d", rc);
        exit(1);
    }
}

/* When set max.message.bytes, we need set the topic znode and create a new znode in /config/changes */
void set_max_message_bytes(long long set_val) {
    serverAssert(set_val > 0);

    zh = zookeeper_init(server.zk_server, NULL, 10000, NULL, NULL, 0);
    if (!zh) {
        serverLog(LL_WARNING, "set_max_message_bytes() failed, zk_server = %s, errno = %d\n", 
                  server.zk_server, errno);
        exit(1);
    } 

    char *stream_write_znode = "/config/topics/redisStreamWrite";

    int buf_len = sizeof(buffer);
    int rc;
    
    rc = zoo_get(zh, stream_write_znode, 0, buffer, &buf_len, &stat);
    if (rc != ZOK || buf_len == -1) {
        serverLog(LL_WARNING, "set_max_message_bytes() failed for zoo_get, znode = %s, zk_server = %s, rc = %d, buf_len = %d", 
                  stream_write_znode, server.zk_server, rc, buf_len);
        exit(1);
    }

    json_error_t err;
    json_t *root = json_loadb(buffer, buf_len, 0, &err);
    if (!root || json_typeof(root) != JSON_OBJECT) {
        serverLog(LL_WARNING, "set_max_message_bytes() failed for root json, buffer = %s", buffer);
        exit(1);
    }

    json_t *config = json_object_get(root, "config");
    if (!config || json_typeof(config) != JSON_OBJECT) {
        serverLog(LL_WARNING, "set_max_message_bytes() failed for config json, buffer = %s", buffer);
        exit(1);
    }

    json_t *max_message_bytes = json_object_get(config, "max.message.bytes");
    if (max_message_bytes) {
        if (json_typeof(max_message_bytes) != JSON_STRING) {
            serverLog(LL_WARNING, "set_max_message_bytes() failed for max_message_bytes json, buffer = %s", buffer);
            exit(1);
        }
        const char *max_message_bytes_str = json_string_value(max_message_bytes);
        long long saved = strtoll(max_message_bytes_str, NULL, 10);
        if (saved == 0 && errno == EINVAL) {
            serverLog(LL_WARNING, "set_max_message_bytes() failed for strtoll, buffer = %s", buffer);
            exit(1);
        }

        if (saved >= set_val) {
            zookeeper_close(zh);
            return;     // check succesfully
        }
        
        // otherwise, we need to increase max.message.bytes
        sds set_val_str = sdsfromlonglong(set_val);
        int ret = json_string_set(max_message_bytes, set_val_str);
        if (ret != 0) {
            serverLog(LL_WARNING, "set_max_message_bytes() failed for json_string_set, set_val_str = %s", set_val_str);
            exit(1);
        }
        sdsfree(set_val_str);

    } else {
        // not found, it is OK to create and set
        sds set_val_str = sdsfromlonglong(set_val);
        json_t *max_message_bytes = json_string(set_val_str);
        if (!max_message_bytes) {
            serverLog(LL_WARNING, "set_max_message_bytes() failed for json_string, set_val_str = %s", set_val_str);
            exit(1);
        }
        int ret = json_object_set(config, "max.message.bytes", max_message_bytes);
        if (ret != 0) {
            serverLog(LL_WARNING, "set_max_message_bytes() failed for json_object_set for max_message_bytes");
            exit(1);
        }
        sdsfree(set_val_str);
    }

    char* marshall_for_root = json_dumps(root, JSON_COMPACT);
    if (!marshall_for_root) {
        serverLog(LL_WARNING, "set_max_message_bytes() failed for json_dumps!");
        exit(1);
    }

    rc = zoo_set(zh, stream_write_znode, marshall_for_root, strlen(marshall_for_root), -1);
    if (rc != ZOK) {
        serverLog(LL_WARNING, "set_max_message_bytes() failed for zoo_set! stream_write_znode = %s, marshall_for_root = %s, rc = %d",
                  stream_write_znode, marshall_for_root, rc);
        exit(1);
    }

    create_change_znode(zh);

    zfree(marshall_for_root);
    zookeeper_close(zh);
}

/* When set max.message.bytes, we need set the topic znode and create a new znode in /config/changes */
void set_compression_type_for_topic() {
    zh = zookeeper_init(server.zk_server, NULL, 10000, NULL, NULL, 0);
    if (!zh) {
        serverLog(LL_WARNING, "set_compression_type_for_topic() failed, zk_server = %s, errno = %d\n", 
                  server.zk_server, errno);
        exit(1);
    } 

    char *stream_write_znode = "/config/topics/redisStreamWrite";

    int buf_len = sizeof(buffer);
    int rc;
    
    rc = zoo_get(zh, stream_write_znode, 0, buffer, &buf_len, &stat);
    if (rc != ZOK || buf_len == -1) {
        serverLog(LL_WARNING, "set_compression_type_for_topic() failed for zoo_get, znode = %s, zk_server = %s, rc = %d, buf_len = %d", 
                  stream_write_znode, server.zk_server, rc, buf_len);
        exit(1);
    }

    json_error_t err;
    json_t *root = json_loadb(buffer, buf_len, 0, &err);
    if (!root || json_typeof(root) != JSON_OBJECT) {
        serverLog(LL_WARNING, "set_compression_type_for_topic() failed for root json, buffer = %s", buffer);
        exit(1);
    }

    json_t *config = json_object_get(root, "config");
    if (!config || json_typeof(config) != JSON_OBJECT) {
        serverLog(LL_WARNING, "set_compression_type_for_topic() failed for config json, buffer = %s", buffer);
        exit(1);
    }

    json_t *compression_type = json_object_get(config, "compression.type");
    if (compression_type) {
        if (json_typeof(compression_type) != JSON_STRING) {
            serverLog(LL_WARNING, "set_compression_type_for_topic() failed for compression.type json, buffer = %s", buffer);
            exit(1);
        }
        const char *compression_type_str = json_string_value(compression_type);

        if (strcasecmp(compression_type_str, "lz4") == 0) {
            zookeeper_close(zh);
            return;     // check succesfully
        }

        // otherwise, we need to update topic compression type to lz4
        int ret = json_string_set(compression_type, "lz4");
        if (ret != 0) {
            serverLog(LL_WARNING, "set_compression_type_for_topic() failed for json_string_set");
            exit(1);
        }
    } else {
        // not found, it is OK to create
        json_t *lz4 = json_string("lz4");
        int ret = json_object_set(config, "compression.type", lz4);
        if (ret != 0) {
            serverLog(LL_WARNING, "set_compression_type_for_topic() failed for json_object_set");
            exit(1);
        }
    }

    char* marshall_for_root = json_dumps(root, JSON_COMPACT);
    if (!marshall_for_root) {
        serverLog(LL_WARNING, "set_compression_type_for_topic() failed for json_dumps!");
        exit(1);
    }

    rc = zoo_set(zh, stream_write_znode, marshall_for_root, strlen(marshall_for_root), -1);
    if (rc != ZOK) {
        serverLog(LL_WARNING, "set_compression_type_for_topic() failed for zoo_set! stream_write_znode = %s, marshall_for_root = %s, rc = %d",
                  stream_write_znode, marshall_for_root, rc);
        exit(1);
    }

    create_change_znode(zh);

    zfree(marshall_for_root);
    zookeeper_close(zh);
}