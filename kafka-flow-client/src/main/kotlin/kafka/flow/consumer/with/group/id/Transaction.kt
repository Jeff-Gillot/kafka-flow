package kafka.flow.consumer.with.group.id

import org.apache.kafka.common.TopicPartition

public sealed interface MaybeTransaction {
    public fun lock()
    public fun unlock()
    public fun rollback()
    public suspend fun register()
}

public object WithoutTransaction : MaybeTransaction {
    override suspend fun register() {}
    override fun lock() {}
    override fun unlock() {}
    override fun rollback() {}
}

public class WithTransaction(
    private val topicPartition: TopicPartition,
    private val offset: Long,
    private val transactionManager: TransactionManager
) : MaybeTransaction {
    public override fun lock() {
        transactionManager.increaseTransaction(topicPartition, offset)
    }

    public override fun unlock() {
        transactionManager.decreaseTransaction(topicPartition, offset)
    }

    public override fun rollback() {
        transactionManager.markRollback(topicPartition)
    }

    override suspend fun register() {
        transactionManager.registerTransaction(topicPartition, offset)
    }

    override fun toString(): String {
        return "Transaction($topicPartition@$offset)"
    }
}