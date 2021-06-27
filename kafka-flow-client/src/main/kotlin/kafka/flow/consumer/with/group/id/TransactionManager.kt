package kafka.flow.consumer.with.group.id

import kafka.flow.utils.seconds
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

public class TransactionManager(private val maxOpenTransactions: Int) {
    private var openedTransactionCount: Int = 0
    private val logger = LoggerFactory.getLogger(TransactionManager::class.java)
    private val openTransactions = HashMap<TopicPartition, SortedMap<Long, AtomicInteger>>()
    private var topicPartitionToRollback = mutableSetOf<TopicPartition>()
    private val mutex = Mutex()

    private suspend fun waitTransactionSlotIfNeeded(topicPartition: TopicPartition, offset: Long) {
        val transactionAlreadyExists = mutex.withLock { transactionsOf(topicPartition).containsKey(offset) }
        if (transactionAlreadyExists) return

        var logTime = Instant.now() + 10.seconds()
        while (openedTransactionCount >= maxOpenTransactions) {
            if (logTime > Instant.now()) {
                logger.info("Too many transactions open, unable to create a transaction for $topicPartition@$offset, waiting until a slot is available")
                logTime = Instant.now() + 10.seconds()
            }
            delay(1)
        }
    }

    public suspend fun increaseTransaction(topicPartition: TopicPartition, offset: Long) {
        waitTransactionSlotIfNeeded(topicPartition, offset)
        mutex.withLock {
            val transaction = transactionsOf(topicPartition).computeIfAbsent(offset) { AtomicInteger() }
            if (transaction.incrementAndGet() == 1) openedTransactionCount++
        }
    }

    public suspend fun decreaseTransaction(topicPartition: TopicPartition, offset: Long) {
        mutex.withLock {
            val transaction = transactionsOf(topicPartition).computeIfAbsent(offset) { AtomicInteger() }
            if (transaction.decrementAndGet() == 0) openedTransactionCount--
        }
    }

    public suspend fun rollbackAndCommit(client: KafkaFlowConsumerWithGroupIdImpl) {
        client.rollback(getPartitionsToRollback())
        client.commit(getOffsetsToCommit())
    }

    private suspend fun getOffsetsToCommit() = mutex.withLock {
        val offsetsToCommit = computeOffsetsToCommit()
        cleanFinishedTransactions()
        offsetsToCommit
    }

    private suspend fun getPartitionsToRollback() = mutex.withLock {
        val partitionsToRollback = topicPartitionToRollback
        topicPartitionToRollback = mutableSetOf()
        partitionsToRollback.forEach { topicPartition -> openTransactions.remove(topicPartition) }
        openedTransactionCount = openTransactions.values.flatMap { it.values }.map { it.get() }.count { it >= 0 }
        partitionsToRollback
    }

    private fun cleanFinishedTransactions() {
        openTransactions.values.forEach { transactionMap ->
            transactionMap.filterValues { it.get() <= 0 }.forEach { transactionMap.remove(it.key) }
        }
    }

    private fun computeOffsetsToCommit(): Map<TopicPartition, OffsetAndMetadata> {
        return openTransactions
            .mapNotNull { (topicPartition, transactionMap) ->
                transactionMap
                    .entries
                    .takeWhile { it.value.get() <= 0 }
                    .lastOrNull()
                    ?.let { topicPartition to OffsetAndMetadata(it.key + 1) }
            }
            .toMap()
    }

    public suspend fun markRollback(topicPartition: TopicPartition): Unit = mutex.withLock {
        topicPartitionToRollback.add(topicPartition)
    }

    private fun transactionsOf(topicPartition: TopicPartition): SortedMap<Long, AtomicInteger> {
        return openTransactions.computeIfAbsent(topicPartition) { TreeMap() }
    }
}