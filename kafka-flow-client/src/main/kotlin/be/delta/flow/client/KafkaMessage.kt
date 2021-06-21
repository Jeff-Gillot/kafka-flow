package be.delta.flow.client

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

public sealed interface KafkaMessage<Key, PartitionKey, Value, Output>

public data class Record<Key, PartitionKey, Value, Output>(
    val original: ConsumerRecord<ByteArray, ByteArray>,
    val key: Key,
    val partitionKey: PartitionKey,
    val value: Value,
    val output: Output
) : KafkaMessage<Key, PartitionKey, Value, Output>

public interface PartitionChangedMessage : KafkaMessage<Unit, Unit, Unit, Unit>
public data class PartitionsAssigned(
    val newlyAssignedPartitions: List<TopicPartition>,
    val newAssignment: List<TopicPartition>
) : KafkaMessage<Unit, Unit, Unit, Unit>, PartitionChangedMessage

public data class PartitionsRevoked(
    val revokedPartitions: List<TopicPartition>,
    val newAssignment: List<TopicPartition>
) : KafkaMessage<Unit, Unit, Unit, Unit>, PartitionChangedMessage

public interface FlowControlMessage : KafkaMessage<Unit, Unit, Unit, Unit>
public object StartConsuming : KafkaMessage<Unit, Unit, Unit, Unit>, FlowControlMessage
public object StopConsuming : KafkaMessage<Unit, Unit, Unit, Unit>, FlowControlMessage
public object EndOfBatch : KafkaMessage<Unit, Unit, Unit, Unit>, FlowControlMessage

