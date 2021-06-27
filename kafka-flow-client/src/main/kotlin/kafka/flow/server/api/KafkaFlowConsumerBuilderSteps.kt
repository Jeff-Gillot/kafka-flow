package kafka.flow.server.api

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.AutoStopPolicy
import kafka.flow.consumer.KafkaMessageWithTransaction
import kafka.flow.consumer.StartOffsetPolicy
import kafka.flow.consumer.with.group.id.KafkaFlowConsumerWithGroupId
import kafka.flow.utils.seconds
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.time.Duration
import java.time.Instant
import java.util.*


public class ConsumerBuilderStep1GroupId<Key, PartitionKey, Value> internal constructor(private val consumerBuilderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>) {
    public fun withGroupId(groupId: String): ConsumerBuilderStep2FromWithGroupId<Key, PartitionKey, Value> = ConsumerBuilderStep2FromWithGroupId(consumerBuilderConfig.copy(groupId = groupId))
    public fun withoutGroupId(groupId: String): Unit = TODO()
}

public class ConsumerBuilderStep2FromWithGroupId<Key, PartitionKey, Value> internal constructor(private val consumerBuilderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>) {
    public fun autoOffsetResetEarliest(maxOpenTransactions: Int = 1024, commitInterval: Duration = 30.seconds()): ConsumerBuilderStep3AutoStopWithGroupIdAndTransaction<Key, PartitionKey, Value> =
        ConsumerBuilderStep3AutoStopWithGroupIdAndTransaction(consumerBuilderConfig.copy(startOffsetPolicy = StartOffsetPolicy.earliest(), maxOpenTransactions = maxOpenTransactions, commitInterval = commitInterval))

    public fun autoOffsetResetLatest(maxOpenTransactions: Int = 1024, commitInterval: Duration = 30.seconds()): ConsumerBuilderStep3AutoStopWithGroupIdAndTransaction<Key, PartitionKey, Value> =
        ConsumerBuilderStep3AutoStopWithGroupIdAndTransaction(consumerBuilderConfig.copy(startOffsetPolicy = StartOffsetPolicy.latest(), maxOpenTransactions = maxOpenTransactions, commitInterval = commitInterval))

    public fun startFromSpecificTime(offsetTime: Instant): Unit = TODO()
    public fun startFromSpecificTimeOffsetFromNow(duration: Duration): Unit = TODO()
}

public class ConsumerBuilderStep3AutoStopWithGroupIdAndTransaction<Key, PartitionKey, Value> internal constructor(private val consumerBuilderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>) {
    public fun consumeUntilStopped(): KafkaFlowConsumerWithGroupId<KafkaMessageWithTransaction<Key, PartitionKey, Value?, Unit>> = consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.never()))
    public fun consumeUntilUpToDate(): KafkaFlowConsumerWithGroupId<KafkaMessageWithTransaction<Key, PartitionKey, Value?, Unit>> = consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.whenUpToDate()))
    public fun consumerUntilSpecifiedTime(stopTime: Instant): KafkaFlowConsumerWithGroupId<KafkaMessageWithTransaction<Key, PartitionKey, Value?, Unit>> = consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.atSpecificTime(stopTime)))
    public fun consumerUntilSpecifiedOffsetFromNow(duration: Duration): KafkaFlowConsumerWithGroupId<KafkaMessageWithTransaction<Key, PartitionKey, Value?, Unit>> = consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.specificOffsetFromNow(duration)))

    private fun consumer(builderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>): KafkaFlowConsumerWithGroupIdAndTransactions<Key, PartitionKey, Value> {
        return KafkaFlowConsumerWithGroupIdAndTransactions(properties(), builderConfig.topicDescriptor, builderConfig.startOffsetPolicy!!, builderConfig.autoStopPolicy!!, builderConfig.maxOpenTransactions!!, builderConfig.commitInterval!!)
    }

    private fun properties(): Properties {
        val properties = Properties()
        properties.putAll(consumerBuilderConfig.serverProperties)
        properties[ConsumerConfig.GROUP_ID_CONFIG] = consumerBuilderConfig.groupId
        properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        return properties
    }
}

public data class ConsumerBuilderConfig<Key, PartitionKey, Value>(
    val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>,
    val serverProperties: Properties,
    val startOffsetPolicy: StartOffsetPolicy? = null,
    val autoStopPolicy: AutoStopPolicy? = null,
    val groupId: String? = null,
    val maxOpenTransactions: Int? = null,
    val commitInterval: Duration? = null,
)