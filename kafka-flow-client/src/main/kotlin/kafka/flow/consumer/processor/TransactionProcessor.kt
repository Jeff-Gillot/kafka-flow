package kafka.flow.consumer.processor

import java.time.Duration
import java.time.Instant
import kafka.flow.consumer.KafkaFlowConsumer
import kafka.flow.consumer.KafkaFlowConsumerWithGroupId
import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.Record
import kafka.flow.consumer.with.group.id.TransactionManager
import kafka.flow.consumer.with.group.id.WithTransaction
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kafka.flow.utils.logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

public class TransactionProcessor<Key, PartitionKey, Value, Output>(
    maxOpenTransactions: Int, private val commitInterval: Duration
) : TransformProcessor<Key, PartitionKey, Value, Output, WithoutTransaction, Key, PartitionKey, Value, Output, WithTransaction> {

    private val transactionManager = TransactionManager(maxOpenTransactions)
    private var client: KafkaFlowConsumerWithGroupId<*>? = null
    private var commitLoop: Job? = null
    private val logger = logger()

    override suspend fun record(
        consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
        key: Key,
        partitionKey: PartitionKey,
        value: Value,
        timestamp: Instant,
        output: Output,
        transaction: WithoutTransaction
    ): Record<Key, PartitionKey, Value, Output, WithTransaction> {
        val newTransaction = WithTransaction(TopicPartition(consumerRecord.topic(), consumerRecord.partition()), consumerRecord.offset(), transactionManager)
        newTransaction.register()
        return Record(
            consumerRecord,
            key,
            partitionKey,
            value,
            timestamp,
            output,
            newTransaction
        )
    }

    override suspend fun startConsuming(client: KafkaFlowConsumer<Flow<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>>) {
        require(client is KafkaFlowConsumerWithGroupId<*>) { "Creation of transaction can only be done when using a groupId consumer" }
        this.client = client

        commitLoop = CoroutineScope(currentCoroutineContext()).launch {
            try {
                while (isActive) {
                    try {
                        delay(commitInterval.toMillis())
                        transactionManager.rollbackAndCommit(client)
                    } catch (cancellation: CancellationException) {
                    } catch (exception: Throwable) {
                        logger.error("Error while trying to commit the offsets to the server", exception)
                    }
                }
            } finally {
                logger.warn("Commit manager is stopping")
            }
        }
    }

    override suspend fun completion() {
        commitLoop?.cancel()
        client?.let { transactionManager.rollbackAndCommit(it) }
    }

    override suspend fun partitionRevoked(revokedPartition: List<TopicPartition>, assignment: List<TopicPartition>) {
        transactionManager.removePartition(revokedPartition)
    }

    override suspend fun partitionAssigned(newlyAssignedPartitions: List<TopicPartition>, assignment: List<TopicPartition>) {
        transactionManager.removePartition(newlyAssignedPartitions)
    }
}