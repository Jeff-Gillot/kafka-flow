package kafka.flow.consumer.with.group.id

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kafka.flow.utils.logger
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
    public val topicPartition: TopicPartition,
    public val offset: Long,
    public val transactionManager: TransactionManager,
    public val transactionTime: Instant = Instant.now(),
) : MaybeTransaction, Comparable<WithTransaction> {
    private val locks: AtomicInteger = AtomicInteger(1)
    private var closed: Boolean = false
    public val stackTraces: MutableList<List<StackTraceElement>> = mutableListOf<List<StackTraceElement>>()

    public override fun lock() {
        synchronized(stackTraces) { stackTraces.add(Exception().stackTrace.toList()) }
        if (closed) {
            logger.error("[Transaction][$this] Locking an already closed transaction", Exception())
        } else {
            locks.incrementAndGet()
        }
    }

    public override fun unlock() {
        synchronized(stackTraces) { stackTraces.add(Exception().stackTrace.toList()) }
        if (closed) {
            logger.error("[Transaction][$this] Unlocking an already closed transaction", Exception())
        } else {
            if (locks.decrementAndGet() == 0) {
                closed = true
                transactionManager.close(this)
            }
        }
    }

    public override fun rollback() {
        synchronized(stackTraces) { stackTraces.add(Exception().stackTrace.toList()) }
        transactionManager.markRollback(topicPartition)
    }

    override suspend fun register() {
        synchronized(stackTraces) { stackTraces.add(Exception().stackTrace.toList()) }
        if (closed) {
            logger.error("[Transaction][$this] Trying to register a closed transaction", Exception())
        } else {
            transactionManager.register(this)
        }
    }

    override fun toString(): String {
        return "Transaction($topicPartition@$offset)/$transactionTime/${locks.get()}"
    }

    private companion object {
        private val logger = logger()
    }

    override fun compareTo(other: WithTransaction): Int {
        if (this == other) return 0
        if (topicPartition != other.topicPartition) return "$topicPartition".compareTo("${other.topicPartition}")
        return offset.compareTo(other.offset)
    }

}