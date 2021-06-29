package kafka.flow.server.api

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.*
import kafka.flow.consumer.KafkaFlowConsumerWithoutGroupId
import kafka.flow.consumer.without.group.id.KafkaFlowConsumerWithoutGroupIdImpl
import kafka.flow.consumer.without.group.id.deserializeUsing
import kafka.flow.utils.allPartitions
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.common.TopicPartition
import java.util.*

public class KafkaFlowConsumerWithoutGroupId<Key, PartitionKey, Value>(
    clientProperties: Properties,
    private val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>,
    private val assignment: List<TopicPartition>,
    startOffsetPolicy: StartOffsetPolicy,
    autoStopPolicy: AutoStopPolicy,
) : KafkaFlowConsumerWithoutGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit>> {
    private val delegate = KafkaFlowConsumerWithoutGroupIdImpl(clientProperties, topicDescriptor.allPartitions(), startOffsetPolicy, autoStopPolicy)

    override suspend fun startConsuming(onDeserializationException: suspend (Throwable) -> Unit): Flow<KafkaMessage<Key, PartitionKey, Value?, Unit>> {
        return delegate.startConsuming()
            .deserializeUsing(topicDescriptor, onDeserializationException)
    }

    override fun stop(): Unit = delegate.stop()
    override fun isRunning(): Boolean = delegate.isRunning()
    override suspend fun isUpToDate(): Boolean = delegate.isUpToDate()
    override suspend fun lag(): Long = delegate.lag()
    override fun close(): Unit = delegate.close()
}