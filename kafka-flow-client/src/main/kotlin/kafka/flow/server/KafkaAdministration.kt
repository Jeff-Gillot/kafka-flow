package kafka.flow.server

import kafka.flow.TopicDescriptor
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import java.util.*
import java.util.concurrent.TimeUnit

public class KafkaAdministration(private val properties: Properties) {

    public fun <Key, Partition, Value> createTopics(vararg topicDescriptors: TopicDescriptor<Key, Partition, Value>): Unit = createTopics(topicDescriptors.toList())

    public fun createTopics(topicDescriptors: List<TopicDescriptor<*, *, *>>) {
        withAdminClient {
            val newTopics = topicDescriptors.map { NewTopic(it.name, Optional.of(it.partitionNumber), Optional.empty()) }.toList()
            createTopics(newTopics).all().get(10, TimeUnit.SECONDS)
        }
    }

    public fun withAdminClient(block: AdminClient.() -> Unit) {
        AdminClient.create(properties).use { block.invoke(it) }
    }
}