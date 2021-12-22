package kafka.flow.consumer

import java.io.Closeable
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

public interface KafkaFlowConsumer<ConsumerOutput> {
    public val assignment: List<TopicPartition>

    public suspend fun startConsuming(onDeserializationException: suspend (Throwable) -> Unit = { it.printStackTrace() }): ConsumerOutput
    public fun stop()

    public fun isRunning(): Boolean
    public fun isUpToDate(): Boolean
    public fun lag(): Long?
    public fun lags(): Map<TopicPartition, Long?>?

    public suspend fun waitUntilUpToDate() {
        while (isRunning() && !isUpToDate()) delay(10)
        if (!isRunning()) throw IllegalStateException("Consumer is not running")
    }

    public suspend fun stopAndWaitUntilStopped() {
        stop()
        while (isRunning()) delay(10)
    }
}

public interface KafkaFlowConsumerWithoutGroupId<FlowType> : KafkaFlowConsumer<Flow<FlowType>>, Closeable

public interface KafkaFlowConsumerWithGroupId<FlowType> : KafkaFlowConsumer<Flow<FlowType>>, Closeable {
    public suspend fun commit(offsetsToCommit: Map<TopicPartition, OffsetAndMetadata>, callback: (Result<Map<TopicPartition, OffsetAndMetadata>>) -> Unit)
    public suspend fun rollback(topicPartitionToRollback: Set<TopicPartition>)
}