#ifndef __ZK_H
#define __ZK_H

// API from config.c
int isValidZk(char *val, const char **err);

uint8_t init_zk_and_get_node_id();
void check_or_set_offset(int64_t offset);
sds get_kafka_broker();

#endif