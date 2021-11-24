package kafka.flow.consumer.with.group.id

import be.delta.flow.time.seconds
import java.time.Instant
import java.util.SortedMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicInteger
import kafka.flow.consumer.KafkaFlowConsumerWithGroupId
import kotlinx.coroutines.delay
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

public class TransactionManager(private val maxOpenTransactions: Int) {
    private val logger = LoggerFactory.getLogger(TransactionManager::class.java)
    private var openedTransactionCount = AtomicInteger()
    private val openTransactions = ConcurrentHashMap<TopicPartition, ConcurrentSkipListMap<Long, AtomicInteger>>()
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

    public suspend fun increaseTransaction(topicPartition: TopicPartition, offset: Long) {
        waitTransactionSlotIfNeeded(topicPartition, offset)
        transactionsOf(topicPartition).computeIfPresent(offset) { _, value ->
            if (value.incrementAndGet() == 1) openedTransactionCount.incrementAndGet()
            value
        }
        transactionsOf(topicPartition).computeIfAbsent(offset) {
            openedTransactionCount.incrementAndGet()
            AtomicInteger(1)
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
        client.commit(computeAndRemoveOffsetsToCommit())
    }

    public fun computeAndRemoveOffsetsToCommit(): Map<TopicPartition, OffsetAndMetadata> {
        return openTransactions
            .mapNotNull { (topicPartition, transactionMap) ->
                val highestClosedTransaction: Long? = transactionMap
                    .entries
                    .takeWhile { it.value.get() <= 0 }
                    .map { it.key }
                    .lastOrNull()

                if (highestClosedTransaction != null) {
                    transactionMap
                        .entries
                        .removeIf { it.value.get() == 0 && it.key <= highestClosedTransaction }
                }

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

    private fun transactionsOf(topicPartition: TopicPartition): SortedMap<Long, AtomicInteger> {
        return openTransactions.computeIfAbsent(topicPartition) { ConcurrentSkipListMap() }
    }

    public fun removePartition(revokedPartition: List<TopicPartition>) {
        revokedPartition.forEach {
            openTransactions.remove(it)
        }
    }
}