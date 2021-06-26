package kafka.flow.consumer.with.group.id

import org.apache.kafka.common.TopicPartition

public class Transaction(private val topicPartition: TopicPartition, private val offset: Long, private val transactionManager: TransactionManager) {
    public suspend fun lock() {
        transactionManager.increaseTransaction(topicPartition, offset)
    }

    public suspend fun unlock() {
        transactionManager.decreaseTransaction(topicPartition, offset)
    }

    public suspend fun rollback() {
        transactionManager.markRollback(topicPartition)
    }

    override fun toString(): String {
        return "Transaction($topicPartition@$offset)"
    }
}