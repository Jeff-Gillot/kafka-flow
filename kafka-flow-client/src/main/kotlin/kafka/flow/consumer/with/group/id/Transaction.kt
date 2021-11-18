package kafka.flow.consumer.with.group.id

import org.apache.kafka.common.TopicPartition

public sealed interface MaybeTransaction {
    public suspend fun lock()
    public fun unlock()
    public fun rollback()
}

public object WithoutTransaction : MaybeTransaction {
    override suspend fun lock() {}
    override fun unlock() {}
    override fun rollback() {}
}

public class WithTransaction(
    private val topicPartition: TopicPartition,
    private val offset: Long,
    private val transactionManager: TransactionManager
) : MaybeTransaction {
    public override suspend fun lock() {
        transactionManager.increaseTransaction(topicPartition, offset)
    }

    public override fun unlock() {
        transactionManager.decreaseTransaction(topicPartition, offset)
    }

    public override fun rollback() {
        transactionManager.markRollback(topicPartition)
    }

    override fun toString(): String {
        return "Transaction($topicPartition@$offset)"
    }
}