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
    for (int i = 0; i <= 255; ++i) {
        sds nodeid_znode = sdsnew(BUNNY_ZK_ROOT_NODE);
        nodeid_znode = sdscatlen(nodeid_znode, "/", 1);
        uint8_t node_id = (uint8_t)i;
        char hex[2];
        byte_to_hex_chars(node_id, hex);
        nodeid_znode = sdscatlen(nodeid_znode, hex, 2);

        int buf_len = sizeof(buffer);
        int rc = zoo_get(zh, nodeid_znode, 0, buffer, &buf_len, &stat);        
        if (rc) {
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
    if (rc) {
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
    if (rc) {
        serverLog(LL_WARNING, "create_znode_for_ip_port() failed, node_id = %d, my_ip_port = %s, zk_server = %s, rc = %d",
                  node_id, my_ip_port, zk_server, rc);
        exit(1);
    }
    sdsfree(my_znode);
}

/* if create root znode, return 1; else return 0 */
static int check_or_create_root_znode(const char* zk_server) {
    sds root_znode = sdsnew(BUNNY_ZK_ROOT_NODE);
    int buf_len = sizeof(buffer);
    int rc = zoo_get(zh, root_znode, 0, buffer, &buf_len, &stat);
    if (rc) {
        if (rc != ZNONODE) {
            serverLog(LL_WARNING, "check_or_set_root_znode() zoo_get failed for %s, zk_server = %s, rc = %d", 
                      root_znode, zk_server, rc);
            exit(1);
        }
        // create root node
        char *root_value = "This is the BunnyRedis root znode!";
        rc = zoo_create(zh, root_znode, root_value, strlen(root_value), 
                        &ZOO_OPEN_ACL_UNSAFE, ZOO_PERSISTENT, buffer, sizeof(buffer)-1);
        if (rc) {
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
    if (rc) {
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
        serverLog(LL_WARNING, "verify_found_node_id() memcpy failed, node_id = %d, ip_port = %s, buffer = %s", 
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
    } else {
        char *search = "\"cleanup.policy\":\"";
        char *start = strstr(buffer, search);
        if (start == NULL)
            return 0;       // not found in buffer
        start += strlen(search);
        char *end = strstr(start, "\"");
        if (end == NULL) {
            serverLog(LL_WARNING, "is_compact_cleanup_policy() faield for search the second quote char!");
            exit(1);
        }
        
        sds cleanup_policy = sdsnewlen(start, end-start);
        if (strcmp(cleanup_policy, "delete,compact") == 0 ||
            strcmp(cleanup_policy, "compact,delete") == 0) {
            serverLog(LL_WARNING, "Kafka cleanup.policy combine delete and compact, it is not recommended!");
            exit(1);
        }

        if (strcmp(cleanup_policy, "compact") == 0) {
            sdsfree(cleanup_policy);
            return 1;
        } else {
            serverAssert(strcmp(cleanup_policy, "delete") == 0);
            sdsfree(cleanup_policy);
            return 0;
        }
    }
}

# define MAX_IP4_LEN    15  // max ip4 is 255.255.255.255    
uint8_t init_zk_and_get_node_id(const char *zk_server) {
    char ip[MAX_IP4_LEN+1];
    get_ip_addresss(ip, MAX_IP4_LEN);
    serverLog(LL_NOTICE, "ip addresss = %s", ip);
    char port[32];
    sprintf(port, "%d", server.port);
    sds my_ip_port = sdsnew(ip);
    my_ip_port = sdscatlen(my_ip_port, ":", 1);
    my_ip_port = sdscatlen(my_ip_port, port, strlen(port));

    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);

    zh = zookeeper_init(zk_server, NULL, 10000, NULL, NULL, 0);
    if (!zh) {
        serverLog(LL_WARNING, "zookeeper_init() failed, zk_server = %s, errno = %d\n", zk_server, errno);
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
    check_or_create_root_znode(zk_server);

    // char *my_ip_port = "127.0.0.1:6379";
    sds my_znode = sdsnew(BUNNY_ZK_ROOT_NODE);
    my_znode = sdscatlen(my_znode, "/", 1);
    my_znode = sdscatlen(my_znode, my_ip_port, strlen(my_ip_port));
    int buf_len = sizeof(buffer);
    int rc = zoo_get(zh, my_znode, 0, buffer, &buf_len, &stat);
    if (rc) {
        if (rc != ZNONODE) {
            serverLog(LL_WARNING, "get zk my node failed for %s, zk_server = %s, rc = %d", my_znode, zk_server, rc);
            exit(1);
        }
        // try to find an available node id
        uint8_t available_id = get_available_node_id();

        // create two znodes for my_zonde and node_id
        create_znode_for_ip_port(available_id, my_ip_port, zk_server);
        create_znode_for_node_id(available_id, my_ip_port, zk_server);

        sdsfree(my_znode);
        sdsfree(my_ip_port);
        return available_id;

    } else {
        // we found my node, return the node id with it
        if (buf_len != 1) {
            serverLog(LL_WARNING, "get zk my node failed, bu_len != 1,  buf_len = %d, for %s, zk_server = %s, rc = %d", 
                      buf_len, my_znode, zk_server, rc);
            exit(1);
        }
        
        uint8_t node_id = buffer[0];

        // we need check reversely by lookup the znode of node_id and verify my_ip_port
        verify_found_node_id(node_id, my_ip_port);

        sdsfree(my_znode);
        sdsfree(my_ip_port);
        return node_id;
    }
}