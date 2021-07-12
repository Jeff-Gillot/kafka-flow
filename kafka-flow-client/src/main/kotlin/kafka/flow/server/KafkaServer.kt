package kafka.flow.server

import kafka.flow.TopicDescriptor
import kafka.flow.producer.KafkaFlowTopicProducer
import org.apache.kafka.clients.CommonClientConfigs
import java.util.*

@Suppress("UNCHECKED_CAST")
public open class KafkaServer(private val config: Properties) {

    public constructor(block: ServerBuilder.() -> Unit) : this(ServerBuilder().apply(block).build())
    public constructor(bootstrapUrl: String) : this(ServerBuilder().apply { this.bootstrapUrl = bootstrapUrl }.build())

    private val producers = mutableMapOf<TopicDescriptor<*, *, *>, KafkaFlowTopicProducer<*, *, *>>()

    public fun <Key, PartitionKey, Value> on(topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>): KafkaFlowTopicProducer<Key, PartitionKey, Value> {
        val producer = producers.computeIfAbsent(topicDescriptor) { KafkaFlowTopicProducer(topicDescriptor, config) }
        return producer as KafkaFlowTopicProducer<Key, PartitionKey, Value>
    }

    public fun <Key, PartitionKey, Value> from(vararg topicDescriptors: TopicDescriptor<out Key, out PartitionKey, out Value>): KafkaFlowTopicReaderBuilder<Key, PartitionKey, Value> {
        return KafkaFlowTopicReaderBuilder(topicDescriptors.toList() as List<TopicDescriptor<Key, PartitionKey, Value>>, config)
    }

    public fun <Key, PartitionKey, Value> from(topicDescriptors: List<TopicDescriptor<out Key, out PartitionKey, out Value>>): KafkaFlowTopicReaderBuilder<Key, PartitionKey, Value> {
        return KafkaFlowTopicReaderBuilder(topicDescriptors as List<TopicDescriptor<Key, PartitionKey, Value>>, config)
    }

    public fun admin(): KafkaAdministration {
        return KafkaAdministration(config)
    }

    public fun properties(): Properties = Properties().apply { putAll(config) }


    public class ServerBuilder {
        public var bootstrapUrl: String? = null

        public fun build(): Properties {
            requireNotNull(bootstrapUrl) { "Bootstrap url needs to be configured" }
            val properties = Properties()
            properties[CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG] = bootstrapUrl
            return properties
        }
    }
}