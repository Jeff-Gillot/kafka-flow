package kafka.flow.server.api

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.*
import kafka.flow.consumer.KafkaFlowConsumerWithoutGroupId
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kafka.flow.consumer.without.group.id.KafkaFlowConsumerWithoutGroupIdImpl
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.common.TopicPartition
import java.util.*

public class KafkaFlowConsumerWithoutGroupId<Key, PartitionKey, Value>(
    clientProperties: Properties,
    private val topicDescriptors: List<TopicDescriptor<Key, PartitionKey, Value>>,
    assignment: List<TopicPartition>,
    startOffsetPolicy: StartOffsetPolicy,
    autoStopPolicy: AutoStopPolicy,
) : KafkaFlowConsumerWithoutGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> {
    private val delegate = KafkaFlowConsumerWithoutGroupIdImpl(clientProperties, assignment, startOffsetPolicy, autoStopPolicy)

    override suspend fun startConsuming(onDeserializationException: suspend (Throwable) -> Unit): Flow<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> {
        return delegate.startConsuming()
            .deserializeUsing(topicDescriptors, onDeserializationException)
    }

    override fun stop(): Unit = delegate.stop()
    override fun isRunning(): Boolean = delegate.isRunning()
    override fun isUpToDate(): Boolean = delegate.isUpToDate()
    override fun lag(): Long? = delegate.lag()
    override fun close(): Unit = delegate.close()
}