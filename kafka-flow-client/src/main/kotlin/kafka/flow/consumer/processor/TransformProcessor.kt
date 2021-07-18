package kafka.flow.consumer.processor

import kafka.flow.consumer.KafkaFlowConsumer
import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.Record
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.consumer.with.group.id.WithoutTransaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.time.Instant

public interface TransformProcessor<KeyIn, PartitionKeyIn, ValueIn, OutputIn, TransactionIn : MaybeTransaction, KeyOut, PartitionKeyOut, ValueOut, OutputOut, TransactionOut : MaybeTransaction> {
    public suspend fun record(
        consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
        key: KeyIn,
        partitionKey: PartitionKeyIn,
        value: ValueIn,
        timestamp: Instant,
        output: OutputIn,
        transaction: TransactionIn
    ): Record<KeyOut, PartitionKeyOut, ValueOut, OutputOut, TransactionOut>?

    public suspend fun startConsuming(client: KafkaFlowConsumer<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>) {}
    public suspend fun stopConsuming() {}
    public suspend fun endOfBatch() {}
    public suspend fun completion() {}

    public suspend fun partitionRevoked(revokedPartition: List<TopicPartition>, assignment: List<TopicPartition>) {}
    public suspend fun partitionAssigned(newlyAssignedPartitions: List<TopicPartition>, assignment: List<TopicPartition>) {}
}