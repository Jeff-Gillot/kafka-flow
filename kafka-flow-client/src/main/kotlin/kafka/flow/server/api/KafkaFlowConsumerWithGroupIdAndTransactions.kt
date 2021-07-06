package kafka.flow.server.api

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.*
import kafka.flow.consumer.KafkaFlowConsumerWithGroupId
import kafka.flow.consumer.with.group.id.KafkaFlowConsumerWithGroupIdImpl
import kafka.flow.consumer.with.group.id.WithTransaction
import kafka.flow.consumer.deserializeUsing
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.*

public class KafkaFlowConsumerWithGroupIdAndTransactions<Key, PartitionKey, Value>(
    clientProperties: Properties,
    private val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>,
    startOffsetPolicy: StartOffsetPolicy,
    autoStopPolicy: AutoStopPolicy,
    private val maxOpenTransactions: Int,
    private val commitInterval: Duration
) : KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithTransaction>> {
    private val delegate = KafkaFlowConsumerWithGroupIdImpl(clientProperties, listOf(topicDescriptor.name), startOffsetPolicy, autoStopPolicy)

    override suspend fun startConsuming(onDeserializationException: suspend (Throwable) -> Unit): Flow<KafkaMessage<Key, PartitionKey, Value?, Unit, WithTransaction>> {
        return delegate
            .startConsuming()
            .deserializeUsing(topicDescriptor, onDeserializationException)
            .createTransactions(maxOpenTransactions, commitInterval)
    }

    override fun stop(): Unit = delegate.stop()
    override fun isRunning(): Boolean = delegate.isRunning()
    override suspend fun isUpToDate(): Boolean = delegate.isUpToDate()
    override suspend fun lag(): Long = delegate.lag()
    override suspend fun commit(commitOffsets: Map<TopicPartition, OffsetAndMetadata>): Unit = delegate.commit(commitOffsets)
    override suspend fun rollback(topicPartitionToRollback: Set<TopicPartition>): Unit = delegate.rollback(topicPartitionToRollback)
    override fun close(): Unit = delegate.close()

}