package kafka.flow.server.api

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.*
import kafka.flow.consumer.with.group.id.KafkaFlowConsumerWithGroupIdImpl
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.*

public class KafkaFlowConsumerWithGroupIdAndWithoutTransactions<Key, PartitionKey, Value>(
    clientProperties: Properties,
    private val topicDescriptors: List<TopicDescriptor<Key, PartitionKey, Value>>,
    startOffsetPolicy: StartOffsetPolicy,
    autoStopPolicy: AutoStopPolicy,
) : KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> {
    private val delegate = KafkaFlowConsumerWithGroupIdImpl(clientProperties, topicDescriptors.map { it.name }, startOffsetPolicy, autoStopPolicy)

    override suspend fun startConsuming(onDeserializationException: suspend (Throwable) -> Unit): Flow<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> {
        return delegate
            .startConsuming()
            .deserializeUsing(topicDescriptors, onDeserializationException)
    }

    override fun stop(): Unit = delegate.stop()
    override fun isRunning(): Boolean = delegate.isRunning()
    override fun isUpToDate(): Boolean = delegate.isUpToDate()
    override fun lag(): Long? = delegate.lag()
    override fun lags(): Map<TopicPartition, Long?>? = delegate.lags()
    override suspend fun commit(commitOffsets: Map<TopicPartition, OffsetAndMetadata>): Unit = delegate.commit(commitOffsets)
    override suspend fun rollback(topicPartitionToRollback: Set<TopicPartition>): Unit = delegate.rollback(topicPartitionToRollback)
    override fun close(): Unit = delegate.close()
    override val assignment: List<TopicPartition> get() = delegate.assignment

}