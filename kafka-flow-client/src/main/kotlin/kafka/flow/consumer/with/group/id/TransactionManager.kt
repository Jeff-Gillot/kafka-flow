package kafka.flow.consumer.with.group.id

import be.delta.flow.time.minutes
import be.delta.flow.time.seconds
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kafka.flow.consumer.KafkaFlowConsumerWithGroupId
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

public class TransactionManager(private val maxOpenTransactions: Int) {
    private val logger = LoggerFactory.getLogger(TransactionManager::class.java)
    private val openedTransactions = ConcurrentHashMap<WithTransaction, Unit>()
    private val highestClosedTransaction = ConcurrentHashMap<TopicPartition, WithTransaction>()
    private var topicPartitionToRollback = mutableSetOf<TopicPartition>()

    public suspend fun register(transaction: WithTransaction) {
        waitTransactionSlotIfNeeded(transaction)
        openedTransactions.compute(transaction) { _, value ->
            if (value != null) {
                logger.error("[TransactionManager] Trying to create a transaction that already exists $transaction", Exception())
            }
            Unit
        }
    }

    public fun close(transaction: WithTransaction) {
        openedTransactions.compute(transaction) { _, value ->
            if (value == null) {
                logger.error("[TransactionManager] Trying to close a transaction that doesn't exists $transaction", Exception())
            }
            highestClosedTransaction.compute(transaction.topicPartition) { _, highestTransaction ->
                if (highestTransaction == null) {
                    transaction
                } else {
                    maxOf(highestTransaction, transaction)
                }
            }
            null
        }
    }

    public suspend fun rollbackAndCommit(client: KafkaFlowConsumerWithGroupId<*>) {
        client.rollback(getPartitionsToRollback())
        val offsetsToCommit = computeOffsetsToCommit(client.assignment)
        client
            .commit(offsetsToCommit)
            .onSuccess { committedOffsets -> removeCommittedOffsets(committedOffsets) }
            .onFailure { logger.error("[TransactionManager] Error while committing offsets ($offsetsToCommit), the system will retry", it) }
    }

    public fun computeOffsetsToCommit(assignment: List<TopicPartition>): Map<TopicPartition, OffsetAndMetadata> {
        openedTransactions.keys.removeIf { it.topicPartition !in assignment }
        highestClosedTransaction.keys.removeIf { it !in assignment }

        val longLastingTransactions = openedTransactions
            .keys
            .filter { it.transactionTime < Instant.now() - 5.minutes() }
        if (longLastingTransactions.isNotEmpty()) {
            logger.warn("[TransactionManager] These transactions have been opened for a long time $longLastingTransactions")
        }

        val minOpenedTransactions = openedTransactions
            .keys
            .filter { it.topicPartition in assignment }
            .groupBy { it.topicPartition }
            .mapValues { (_, transactionsForPartition) -> transactionsForPartition.minByOrNull { it.offset }!! }

        return assignment
            .mapNotNull { topicPartition ->
                when {
                    minOpenedTransactions[topicPartition] != null -> topicPartition to OffsetAndMetadata(minOpenedTransactions[topicPartition]!!.offset)
                    highestClosedTransaction[topicPartition] != null -> topicPartition to OffsetAndMetadata(highestClosedTransaction[topicPartition]!!.offset + 1)
                    else -> null
                }
            }
            .toMap()
    }

    public fun removeCommittedOffsets(committedOffsets: Map<TopicPartition, OffsetAndMetadata>) {
        committedOffsets.forEach { (topicPartition, offsetAndMetadata) ->
            highestClosedTransaction.computeIfPresent(topicPartition) { _, highestTransaction ->
                if (highestTransaction.topicPartition == topicPartition && highestTransaction.offset + 1 == offsetAndMetadata.offset()) {
                    null
                } else {
                    highestTransaction
                }
            }
        }
    }

    private fun getPartitionsToRollback() =
        if (topicPartitionToRollback.isNotEmpty()) {
            val partitionsToRollback = topicPartitionToRollback
            highestClosedTransaction.keys.removeIf { it in partitionsToRollback }
            openedTransactions.keys.removeIf { it.topicPartition in partitionsToRollback }
            topicPartitionToRollback = mutableSetOf()
            partitionsToRollback
        } else {
            emptySet()
        }


    public fun markRollback(topicPartition: TopicPartition) {
        topicPartitionToRollback.add(topicPartition)
    }

    private suspend fun waitTransactionSlotIfNeeded(transaction: WithTransaction) {
        var logTime = Instant.now() + 10.seconds()
        while (openedTransactions.size >= maxOpenTransactions) {
            if (logTime < Instant.now()) {
                logger.warn("Too many transactions open, unable to create a transaction for $transaction, waiting until a slot is available")
                logTime = Instant.now() + 10.seconds()
            }
            delay(10)
        }
    }

    public fun clear() {
        openedTransactions.clear()
        highestClosedTransaction.clear()
    }
}