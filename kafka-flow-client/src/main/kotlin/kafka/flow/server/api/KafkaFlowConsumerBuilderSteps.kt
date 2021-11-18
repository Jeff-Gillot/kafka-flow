package kafka.flow.server.api

import be.delta.flow.time.seconds
import java.time.Duration
import java.time.Instant
import java.util.Properties
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.AutoStopPolicy
import kafka.flow.consumer.KafkaFlowConsumerWithGroupId
import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.StartOffsetPolicy
import kafka.flow.consumer.with.group.id.WithTransaction
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kafka.flow.utils.allPartitions
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition


public class ConsumerBuilderStep1GroupId<Key, PartitionKey, Value> internal constructor(private val consumerBuilderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>) {
    public fun withGroupId(groupId: String): ConsumerBuilderStep2FromWithGroupId<Key, PartitionKey, Value> =
        ConsumerBuilderStep2FromWithGroupId(consumerBuilderConfig.copy(groupId = groupId))

    public fun withoutGroupId(): ConsumerBuilderStep2FromWithoutGroupId<Key, PartitionKey, Value> =
        ConsumerBuilderStep2FromWithoutGroupId(consumerBuilderConfig)
}

public class ConsumerBuilderStep2FromWithGroupId<Key, PartitionKey, Value> internal constructor(private val consumerBuilderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>) {
    public fun autoOffsetResetEarliest(maxOpenTransactions: Int = 1024, commitInterval: Duration = 30.seconds()): ConsumerBuilderStep3AutoStopWithGroupIdAndTransaction<Key, PartitionKey, Value> =
        ConsumerBuilderStep3AutoStopWithGroupIdAndTransaction(
            consumerBuilderConfig.copy(
                startOffsetPolicy = StartOffsetPolicy.earliest(),
                maxOpenTransactions = maxOpenTransactions,
                commitInterval = commitInterval
            )
        )

    public fun autoOffsetResetLatest(maxOpenTransactions: Int = 1024, commitInterval: Duration = 30.seconds()): ConsumerBuilderStep3AutoStopWithGroupIdAndTransaction<Key, PartitionKey, Value> =
        ConsumerBuilderStep3AutoStopWithGroupIdAndTransaction(
            consumerBuilderConfig.copy(
                startOffsetPolicy = StartOffsetPolicy.latest(),
                maxOpenTransactions = maxOpenTransactions,
                commitInterval = commitInterval
            )
        )

    public fun startFromSpecificTime(offsetTime: Instant): ConsumerBuilderStep3AutoStopWithGroupIdAndWithoutTransaction<Key, PartitionKey, Value> =
        ConsumerBuilderStep3AutoStopWithGroupIdAndWithoutTransaction(consumerBuilderConfig.copy(startOffsetPolicy = StartOffsetPolicy.specificTime(offsetTime)))

    public fun startFromSpecificTimeOffsetFromNow(duration: Duration): ConsumerBuilderStep3AutoStopWithGroupIdAndWithoutTransaction<Key, PartitionKey, Value> =
        ConsumerBuilderStep3AutoStopWithGroupIdAndWithoutTransaction(consumerBuilderConfig.copy(startOffsetPolicy = StartOffsetPolicy.specificOffsetFromNow(duration)))
}

public class ConsumerBuilderStep2FromWithoutGroupId<Key, PartitionKey, Value> internal constructor(private val consumerBuilderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>) {
    public fun startFromEarliest(): ConsumerBuilderStep3AutoStopWithoutGroupId<Key, PartitionKey, Value> =
        startFromPolicy(StartOffsetPolicy.earliest())

    public fun startFromLatest(): ConsumerBuilderStep3AutoStopWithoutGroupId<Key, PartitionKey, Value> =
        startFromPolicy(StartOffsetPolicy.latest())

    public fun startFromSpecificTime(offsetTime: Instant): ConsumerBuilderStep3AutoStopWithoutGroupId<Key, PartitionKey, Value> =
        startFromPolicy(StartOffsetPolicy.specificTime(offsetTime))

    public fun startFromSpecificTimeOffsetFromNow(duration: Duration): ConsumerBuilderStep3AutoStopWithoutGroupId<Key, PartitionKey, Value> =
        startFromPolicy(StartOffsetPolicy.specificOffsetFromNow(duration))

    public fun startFromPolicy(startOffsetPolicy: StartOffsetPolicy): ConsumerBuilderStep3AutoStopWithoutGroupId<Key, PartitionKey, Value> =
        ConsumerBuilderStep3AutoStopWithoutGroupId(consumerBuilderConfig.copy(startOffsetPolicy = startOffsetPolicy))
}

public class ConsumerBuilderStep3AutoStopWithGroupIdAndTransaction<Key, PartitionKey, Value> internal constructor(private val consumerBuilderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>) {
    public fun additionalProperty(key: String, value: String): ConsumerBuilderStep3AutoStopWithGroupIdAndTransaction<Key, PartitionKey, Value> {
        consumerBuilderConfig.clientProperties[key] = value
        return this
    }

    public fun consumeUntilStopped(): KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithTransaction>> =
        consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.never()))

    public fun consumeUntilUpToDate(): KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithTransaction>> =
        consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.whenUpToDate()))

    public fun consumerUntilSpecifiedTime(stopTime: Instant): KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithTransaction>> =
        consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.atSpecificTime(stopTime)))

    public fun consumerUntilSpecifiedOffsetFromNow(duration: Duration): KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithTransaction>> =
        consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.specificOffsetFromNow(duration)))

    private fun consumer(builderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>): KafkaFlowConsumerWithGroupIdAndTransactions<Key, PartitionKey, Value> {
        return KafkaFlowConsumerWithGroupIdAndTransactions(
            properties(),
            builderConfig.topicDescriptors,
            builderConfig.startOffsetPolicy!!,
            builderConfig.autoStopPolicy!!,
            builderConfig.maxOpenTransactions!!,
            builderConfig.commitInterval!!
        )
    }

    private fun properties(): Properties {
        val properties = Properties()
        properties.putAll(consumerBuilderConfig.serverProperties)
        properties.putAll(consumerBuilderConfig.clientProperties)
        properties[ConsumerConfig.GROUP_ID_CONFIG] = consumerBuilderConfig.groupId
        properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        return properties
    }
}

public class ConsumerBuilderStep3AutoStopWithGroupIdAndWithoutTransaction<Key, PartitionKey, Value> internal constructor(private val consumerBuilderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>) {
    public fun additionalProperty(key: String, value: String): ConsumerBuilderStep3AutoStopWithGroupIdAndWithoutTransaction<Key, PartitionKey, Value> {
        consumerBuilderConfig.clientProperties[key] = value
        return this
    }

    public fun consumeUntilStopped(): KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> =
        consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.never()))

    public fun consumeUntilUpToDate(): KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> =
        consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.whenUpToDate()))

    public fun consumerUntilSpecifiedTime(stopTime: Instant): KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> =
        consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.atSpecificTime(stopTime)))

    public fun consumerUntilSpecifiedOffsetFromNow(duration: Duration): KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> =
        consumer(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.specificOffsetFromNow(duration)))

    private fun consumer(builderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>): KafkaFlowConsumerWithGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> {
        return KafkaFlowConsumerWithGroupIdAndWithoutTransactions(
            properties(),
            builderConfig.topicDescriptors,
            builderConfig.startOffsetPolicy!!,
            builderConfig.autoStopPolicy!!,
        )
    }

    private fun properties(): Properties {
        val properties = Properties()
        properties.putAll(consumerBuilderConfig.serverProperties)
        properties.putAll(consumerBuilderConfig.clientProperties)
        properties[ConsumerConfig.GROUP_ID_CONFIG] = consumerBuilderConfig.groupId
        properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        return properties
    }
}

public class ConsumerBuilderStep3AutoStopWithoutGroupId<Key, PartitionKey, Value> internal constructor(private val consumerBuilderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>) {
    public fun consumeUntilStopped(): ConsumerBuilderStep4PartitionsWithoutGroupId<Key, PartitionKey, Value> =
        ConsumerBuilderStep4PartitionsWithoutGroupId(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.never()))

    public fun consumeUntilUpToDate(): ConsumerBuilderStep4PartitionsWithoutGroupId<Key, PartitionKey, Value> =
        ConsumerBuilderStep4PartitionsWithoutGroupId(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.whenUpToDate()))

    public fun consumerUntilSpecifiedTime(stopTime: Instant): ConsumerBuilderStep4PartitionsWithoutGroupId<Key, PartitionKey, Value> =
        ConsumerBuilderStep4PartitionsWithoutGroupId(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.atSpecificTime(stopTime)))

    public fun consumerUntilSpecifiedOffsetFromNow(duration: Duration): ConsumerBuilderStep4PartitionsWithoutGroupId<Key, PartitionKey, Value> =
        ConsumerBuilderStep4PartitionsWithoutGroupId(consumerBuilderConfig.copy(autoStopPolicy = AutoStopPolicy.specificOffsetFromNow(duration)))
}

public class ConsumerBuilderStep4PartitionsWithoutGroupId<Key, PartitionKey, Value> internal constructor(private val consumerBuilderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>) {
    public fun additionalProperty(key: String, value: String): ConsumerBuilderStep4PartitionsWithoutGroupId<Key, PartitionKey, Value> {
        consumerBuilderConfig.clientProperties[key] = value
        return this
    }

    public fun readSpecificPartitions(partitions: List<TopicPartition>): kafka.flow.consumer.KafkaFlowConsumerWithoutGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> {
        val topics = consumerBuilderConfig.topicDescriptors.associateBy { it.name }
        partitions.firstOrNull { !topics.containsKey(it.topic()) }?.let {
            throw IllegalArgumentException("Invalid topic partition you are trying to subscribe to a topic that's not in the list of topics '$it', available topics ${topics.keys.joinToString()}")
        }
        partitions.firstOrNull { it.partition() < 0 || it.partition() >= topics[it.topic()]!!.partitionNumber }?.let {
            throw IllegalArgumentException("Invalid partitions numbers it should be between 0 and ${topics[it.topic()]!!.partitionNumber - 1} found $it")
        }

        return consumer(consumerBuilderConfig, partitions)
    }

    public fun readAllPartitions(): kafka.flow.consumer.KafkaFlowConsumerWithoutGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> =
        consumer(consumerBuilderConfig, consumerBuilderConfig.topicDescriptors.flatMap { it.allPartitions() })

    private fun consumer(
        builderConfig: ConsumerBuilderConfig<Key, PartitionKey, Value>,
        partitions: List<TopicPartition>
    ): kafka.flow.consumer.KafkaFlowConsumerWithoutGroupId<KafkaMessage<Key, PartitionKey, Value?, Unit, WithoutTransaction>> {
        return KafkaFlowConsumerWithoutGroupId(
            properties(),
            builderConfig.topicDescriptors,
            partitions,
            builderConfig.startOffsetPolicy!!,
            builderConfig.autoStopPolicy!!,
        )
    }

    private fun properties(): Properties {
        val properties = Properties()
        properties.putAll(consumerBuilderConfig.serverProperties)
        properties.putAll(consumerBuilderConfig.clientProperties)
        properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        return properties
    }
}

public data class ConsumerBuilderConfig<Key, PartitionKey, Value>(
    val topicDescriptors: List<TopicDescriptor<Key, PartitionKey, Value>>,
    val serverProperties: Properties,
    val startOffsetPolicy: StartOffsetPolicy? = null,
    val autoStopPolicy: AutoStopPolicy? = null,
    val groupId: String? = null,
    val maxOpenTransactions: Int? = null,
    val commitInterval: Duration? = null,
    val clientProperties: HashMap<String, String> = HashMap()
)