package kafka.flow.producer

import kafka.flow.TopicDescriptor
import kafka.flow.server.KafkaServer
import java.time.Instant

public data class KafkaOutput(public val records: List<KafkaOutputRecord>) {
    public companion object {
        public fun <Key, PartitionKey, Value> forValue(kafkaServer: KafkaServer, topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>, value: Value): KafkaOutput =
            KafkaOutput(listOf(TopicDescriptorRecord.Record(kafkaServer, topicDescriptor, value)))

        public fun <Key, PartitionKey, Value> forTombstone(kafkaServer: KafkaServer, topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>, key: Key, timestamp: Instant): KafkaOutput =
            KafkaOutput(listOf(TopicDescriptorRecord.Tombstone(kafkaServer, topicDescriptor, key, timestamp)))
    }

    public operator fun plus(other: KafkaOutput): KafkaOutput {
        return KafkaOutput(records + other.records)
    }
}

public typealias KafkaOutputRecord = TopicDescriptorRecord<*, *, *>
public sealed interface TopicDescriptorRecord<Key, PartitionKey, Value> {
    public val key: Key
    public val timestamp: Instant
    public val value: Value?
    public val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>
    public val kafkaServer: KafkaServer

    public data class Tombstone<Key, PartitionKey, Value>(
        override val kafkaServer: KafkaServer,
        override val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>,
        override val key: Key,
        override val timestamp: Instant,
    ) : TopicDescriptorRecord<Key, PartitionKey, Value> {
        override val value: Value? = null
    }

    public data class Record<Key, PartitionKey, Value>(
        override val kafkaServer: KafkaServer,
        override val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>,
        override val value: Value
    ) : TopicDescriptorRecord<Key, PartitionKey, Value> {
        override val key: Key = topicDescriptor.key(value)
        override val timestamp: Instant = topicDescriptor.timestamp(value)
    }
}