package kafka.flow.server

import kafka.flow.TopicDescriptor
import kafka.flow.producer.KafkaFlowTopicProducer
import org.apache.kafka.clients.CommonClientConfigs
import java.util.*

public data class Server(private val config: Properties) {

    public constructor(block: ServerBuilder.() -> Unit) : this(ServerBuilder().apply(block).build())
    public constructor(bootstrapUrl: String) : this(ServerBuilder().apply { this.bootstrapUrl = bootstrapUrl }.build())


    private val producers = mutableMapOf<TopicDescriptor<*, *, *>, KafkaFlowTopicProducer<*, *, *>>()

    public fun <Key, PartitionKey, Value> on(topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>): KafkaFlowTopicProducer<Key, PartitionKey, Value> {
        val producer = producers.computeIfAbsent(topicDescriptor) { KafkaFlowTopicProducer(topicDescriptor, config) }
        @Suppress("UNCHECKED_CAST")
        return producer as KafkaFlowTopicProducer<Key, PartitionKey, Value>
    }

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