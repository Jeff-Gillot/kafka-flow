package kafka.flow.consumer.with.group.id

import be.delta.flow.time.seconds
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kafka.flow.consumer.KafkaFlowConsumerWithGroupId
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

public class TransactionManager(private val maxOpenTransactions: Int) {
    private val logger = LoggerFactory.getLogger(TransactionManager::class.java)
    private var openedTransactionCount = AtomicInteger()
    private val openTransactions = ConcurrentHashMap<TopicPartition, ConcurrentHashMap<Long, AtomicInteger>>()
    private var topicPartitionToRollback = mutableSetOf<TopicPartition>()

    private suspend fun waitTransactionSlotIfNeeded(topicPartition: TopicPartition, offset: Long) {
        val transactionAlreadyExists = transactionsOf(topicPartition).containsKey(offset)
        if (transactionAlreadyExists) return

        var logTime = Instant.now() + 10.seconds()
        while (openedTransactionCount.get() >= maxOpenTransactions) {
            if (logTime < Instant.now()) {
                logger.warn("Too many transactions open, unable to create a transaction for $topicPartition@$offset, waiting until a slot is available")
                logTime = Instant.now() + 10.seconds()
            }
            delay(10)
        }
    }

    public suspend fun registerTransaction(topicPartition: TopicPartition, offset: Long) {
        waitTransactionSlotIfNeeded(topicPartition, offset)
        transactionsOf(topicPartition).computeIfAbsent(offset) {
            openedTransactionCount.incrementAndGet()
            AtomicInteger(1)
        }
    }

    public fun increaseTransaction(topicPartition: TopicPartition, offset: Long) {
        transactionsOf(topicPartition).computeIfPresent(offset) { _, value ->
            if (value.incrementAndGet() == 1) openedTransactionCount.incrementAndGet()
            value
        }
    }

    public fun decreaseTransaction(topicPartition: TopicPartition, offset: Long) {
        transactionsOf(topicPartition).computeIfPresent(offset) { _, value ->
            if (value.decrementAndGet() == 0) openedTransactionCount.decrementAndGet()
            value
        }
    }

    public suspend fun rollbackAndCommit(client: KafkaFlowConsumerWithGroupId<*>) {
        client.rollback(getPartitionsToRollback())
        val offsetsToCommit = computeOffsetsToCommit(client.assignment)
        client.commit(offsetsToCommit)
            .onSuccess { committedOffsets ->
                committedOffsets.forEach { (key, value) ->
                    transactionsOf(key)
                        .entries
                        .removeIf { it.value.get() == 0 && it.key < value.offset() }
                }
            }
            .onFailure {
                logger.error("Error while committing offsets ($offsetsToCommit), the system will retry", it)
            }
    }

    public fun computeOffsetsToCommit(assignment: List<TopicPartition>): Map<TopicPartition, OffsetAndMetadata> {
        openTransactions.entries.removeIf { it.key !in assignment }
        openedTransactionCount = AtomicInteger(openTransactions.values.flatMap { it.values }.sumOf { it.get() })

        return openTransactions
            .filterKeys { it in assignment }
            .mapNotNull { (topicPartition, transactionMap) ->
                val highestClosedTransaction: Long? = transactionMap
                    .entries
                    .sortedBy { it.key }
                    .takeWhile { it.value.get() <= 0 }
                    .map { it.key }
                    .lastOrNull()

                highestClosedTransaction?.let { topicPartition to OffsetAndMetadata(it + 1) }
            }
            .toMap()
    }


    private fun getPartitionsToRollback() =
        if (topicPartitionToRollback.isNotEmpty()) {
            val partitionsToRollback = topicPartitionToRollback
            partitionsToRollback.forEach { topicPartition ->
                openTransactions.remove(topicPartition)
            }
            topicPartitionToRollback = mutableSetOf()
            openedTransactionCount = AtomicInteger(openTransactions.values.flatMap { it.values }.map { it.get() }.count { it >= 0 })
            partitionsToRollback
        } else {
            emptySet()
        }


    public fun markRollback(topicPartition: TopicPartition) {
        topicPartitionToRollback.add(topicPartition)
    }

    private fun transactionsOf(topicPartition: TopicPartition): ConcurrentHashMap<Long, AtomicInteger> {
        return openTransactions.computeIfAbsent(topicPartition) { ConcurrentHashMap() }
    }

    public fun removePartition(revokedPartition: List<TopicPartition>) {
        revokedPartition.forEach {
            openTransactions.remove(it)
        }
    }
}