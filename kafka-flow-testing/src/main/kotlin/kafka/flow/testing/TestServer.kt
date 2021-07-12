package kafka.flow.testing

import kafka.flow.TopicDescriptor
import kafka.flow.server.KafkaServer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName


public object KafkaTestContainer : KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"))
public object TestServer : KafkaServer({
    KafkaTestContainer.start()
    bootstrapUrl = KafkaTestContainer.bootstrapServers
}) {
    public fun <Key, PartitionKey, Value> topicTester(topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>): TopicTester<Key, PartitionKey, Value> =
        TopicTester(topicDescriptor, this)
}