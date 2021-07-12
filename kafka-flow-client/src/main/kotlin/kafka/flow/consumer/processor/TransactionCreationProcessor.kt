package kafka.flow.consumer.processor

import kafka.flow.consumer.KafkaFlowConsumer
import kafka.flow.consumer.KafkaFlowConsumerWithGroupId
import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.Record
import kafka.flow.consumer.with.group.id.TransactionManager
import kafka.flow.consumer.with.group.id.WithTransaction
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.time.Duration

public class TransactionCreationProcessor<Key, PartitionKey, Value, Output>(
    maxOpenTransactions: Int, private val commitInterval: Duration
) : TransformProcessor<Key, PartitionKey, Value, Output, WithoutTransaction, Key, PartitionKey, Value, Output, WithTransaction> {

    private val transactionManager = TransactionManager(maxOpenTransactions)
    private var client: KafkaFlowConsumerWithGroupId<*>? = null
    private var commitLoop: Job? = null

    override suspend fun record(
        consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
        key: Key,
        partitionKey: PartitionKey,
        value: Value,
        output: Output,
        transaction: WithoutTransaction
    ): Record<Key, PartitionKey, Value, Output, WithTransaction> {
        val newTransaction = WithTransaction(TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset(), transactionManager)
        return Record(
            consumerRecord,
            key,
            partitionKey,
            value,
            output,
            newTransaction
        )
    }

    override suspend fun startConsuming(client: KafkaFlowConsumer<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>) {
        require(client is KafkaFlowConsumerWithGroupId) { "Creation of transaction can only be done when using a groupId consumer" }
        this.client = client

        commitLoop = CoroutineScope(currentCoroutineContext()).launch {
            while (true) {
                delay(commitInterval.toMillis())
                transactionManager.rollbackAndCommit(client)
            }
        }
    }

    override suspend fun endOfBatch() {
        client?.let { transactionManager.rollbackAndCommit(it) }
    }

    override suspend fun completion() {
        commitLoop?.cancel()
        client?.let { transactionManager.rollbackAndCommit(it) }
    }
}