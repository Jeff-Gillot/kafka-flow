package kafka.flow.consumer

import kafka.flow.consumer.processor.cache.Cache
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.io.Closeable

public interface KafkaFlowConsumer<FlowType> : KafkaClient {
    public suspend fun startConsuming(onDeserializationException: suspend (Throwable) -> Unit = { it.printStackTrace() }): Flow<FlowType>
}

public interface KafkaClient : Closeable {
    public fun stop()

    public fun isRunning(): Boolean
    public suspend fun isUpToDate(): Boolean
    public suspend fun lag(): Long

    public suspend fun waitUntilUpToDate() {
        while (isRunning() && !isUpToDate()) delay(10)
        if (!isRunning()) throw IllegalStateException("Consumer is not running")
    }

    public suspend fun stopAndWaitUntilStopped() {
        stop()
        while (isRunning()) delay(10)
    }
}

public interface KafkaFlowConsumerWithoutGroupId<FlowType> : KafkaFlowConsumer<FlowType>, Closeable

public interface KafkaFlowConsumerWithGroupId<FlowType> : KafkaFlowConsumer<FlowType>, Closeable {
    public suspend fun commit(commitOffsets: Map<TopicPartition, OffsetAndMetadata>)
    public suspend fun rollback(topicPartitionToRollback: Set<TopicPartition>)
}