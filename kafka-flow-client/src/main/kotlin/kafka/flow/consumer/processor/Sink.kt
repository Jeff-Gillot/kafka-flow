package kafka.flow.consumer.processor

import kafka.flow.consumer.KafkaFlowConsumer
import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.Record
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.consumer.with.group.id.WithTransaction
import kafka.flow.consumer.with.group.id.WithoutTransaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

public interface Sink<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction> {
    public suspend fun record(
        consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
        key: Key,
        partitionKey: PartitionKey,
        value: Value,
        output: Output,
        transaction: Transaction
    )

    public suspend fun startConsuming(client: KafkaFlowConsumer<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>) {}
    public suspend fun stopConsuming() {}
    public suspend fun endOfBatch() {}
    public suspend fun completion() {}

    public suspend fun partitionRevoked(revokedPartition: List<TopicPartition>, assignment: List<TopicPartition>) {}
    public suspend fun partitionAssigned(newlyAssignedPartitions: List<TopicPartition>, assignment: List<TopicPartition>) {}
}