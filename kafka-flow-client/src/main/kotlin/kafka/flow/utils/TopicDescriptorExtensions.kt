package kafka.flow.utils

import kafka.flow.TopicDescriptor
import org.apache.kafka.common.TopicPartition

public fun <Key, PartitionKey, Value> TopicDescriptor<Key, PartitionKey, Value>.allPartitions(): List<TopicPartition> =
    (0 until partitionNumber).map { TopicPartition(name, it) }