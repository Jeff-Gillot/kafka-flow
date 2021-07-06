package kafka.flow.consumer.with.group.id

import org.apache.kafka.common.TopicPartition

public sealed interface MaybeTransaction {
    public suspend fun lock()
    public suspend fun unlock()
    public suspend fun rollback()
}

public object WithoutTransaction : MaybeTransaction {
    override suspend fun lock() {}
    override suspend fun unlock() {}
    override suspend fun rollback() {}
}

public class WithTransaction(
    private val topicPartition: TopicPartition,
    private val offset: Long,
    private val transactionManager: TransactionManager
) : MaybeTransaction {
    public override suspend fun lock() {
        transactionManager.increaseTransaction(topicPartition, offset)
    }

    public override suspend fun unlock() {
        transactionManager.decreaseTransaction(topicPartition, offset)
    }

    public override suspend fun rollback() {
        transactionManager.markRollback(topicPartition)
    }

    override fun toString(): String {
        return "Transaction($topicPartition@$offset)"
    }
}