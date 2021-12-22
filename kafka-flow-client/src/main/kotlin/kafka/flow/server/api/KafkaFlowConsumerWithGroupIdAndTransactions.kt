package kafka.flow.server.api

import java.time.Duration
import java.util.Properties
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.AutoStopPolicy
import kafka.flow.consumer.KafkaFlowConsumerWithGroupId
import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.StartOffsetPolicy
import kafka.flow.consumer.createTransactions
import kafka.flow.consumer.deserializeUsing
import kafka.flow.consumer.with.group.id.KafkaFlowConsumerWithGroupIdImpl
import kafka.flow.consumer.with.group.id.WithTransaction
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

public class KafkaFlowConsumerWithGroupIdAndTransactions<Key, PartitionKey, Value>(
    clientProperties: Properties,
    private val topicDescriptors: List<TopicDescriptor<Key, PartitionKey, Value>>,
    startOffsetPolicy: StartOffsetPolicy,
    autoStopPolicy: AutoStopPolicy,
    private val maxOpenTransactions: Int,
    private val commitInterval: Duration
) : KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithTransaction>> {
    private val delegate = KafkaFlowConsumerWithGroupIdImpl(clientProperties, topicDescriptors.map { it.name }, startOffsetPolicy, autoStopPolicy)

    override suspend fun startConsuming(onDeserializationException: suspend (Throwable) -> Unit): Flow<KafkaMessage<Key, PartitionKey, Value?, Unit, WithTransaction>> {
        return delegate
            .startConsuming()
            .deserializeUsing(topicDescriptors, onDeserializationException)
            .createTransactions(maxOpenTransactions, commitInterval)
    }

    override fun stop(): Unit = delegate.stop()
    override fun isRunning(): Boolean = delegate.isRunning()
    override fun isUpToDate(): Boolean = delegate.isUpToDate()
    override fun lag(): Long? = delegate.lag()
    override fun lags(): Map<TopicPartition, Long?>? = delegate.lags()
    override suspend fun commit(offsetsToCommit: Map<TopicPartition, OffsetAndMetadata>, callback: (Result<Map<TopicPartition, OffsetAndMetadata>>) -> Unit): Unit =
        delegate.commit(offsetsToCommit, callback)

    override suspend fun rollback(topicPartitionToRollback: Set<TopicPartition>): Unit = delegate.rollback(topicPartitionToRollback)
    override fun close(): Unit = delegate.close()
    override val assignment: List<TopicPartition> get() = delegate.assignment

}