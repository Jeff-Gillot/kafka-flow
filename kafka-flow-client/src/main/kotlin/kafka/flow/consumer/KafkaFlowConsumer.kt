package kafka.flow.consumer

import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.io.Closeable

public interface KafkaFlowConsumer<FlowType> : Closeable {
    public suspend fun startConsuming(onDeserializationException: suspend (Throwable) -> Unit = { it.printStackTrace() }): Flow<FlowType>
    public fun stop()

    public fun isRunning(): Boolean
    public suspend fun isUpToDate(): Boolean
    public suspend fun lag(): Long
}

public interface KafkaFlowConsumerWithoutGroupId<FlowType> : KafkaFlowConsumer<FlowType>, Closeable


public interface KafkaFlowConsumerWithGroupId<FlowType> : KafkaFlowConsumer<FlowType>, Closeable {
    public suspend fun commit(commitOffsets: Map<TopicPartition, OffsetAndMetadata>)
    public suspend fun rollback(topicPartitionToRollback: Set<TopicPartition>)
}