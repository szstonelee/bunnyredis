#ifndef __ZK_H
#define __ZK_H

uint8_t init_zk_and_get_node_id();
void check_or_set_offset(int64_t offset);
sds get_kafka_broker();
void set_max_message_bytes(long long set_val);
void set_compression_type_for_topic();

#endif