package kafka.flow.consumer

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.with.group.id.KafkaFlowConsumerWithGroupIdImpl
import kafka.flow.producer.KafkaFlowTopicProducer
import kafka.flow.server.KafkaServer
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestTopicDescriptor
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.*


class KafkaToKafkaIntegrationTest {
    private lateinit var topic: TopicDescriptor<TestObject.Key, String, TestObject>
    private lateinit var producer: KafkaFlowTopicProducer<TestObject.Key, String, TestObject>
    private var consumer: KafkaFlowConsumerWithGroupIdImpl? = null

    @Before
    fun createTestTopic() {
        topic = TestTopicDescriptor.next()
        producer = kafkaServer.on(topic)
        val admin = AdminClient.create(properties())
        admin.createTopics(listOf(NewTopic(topic.name, 12, 1))).all().get()
    }

    @After
    fun after() {
        producer.close()
    }

    @Test
    fun subscribeFromBeginning_readAllMessages(): Unit = runTest {
    }

    private fun properties(): Properties {
        val properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        properties[ConsumerConfig.GROUP_ID_CONFIG] = "test-client"
        properties[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        return properties
    }

    private fun runTest(block: suspend CoroutineScope.() -> Unit) {
        runBlocking {
            try {
                block.invoke(this)
            } finally {
                consumer?.stop()
            }
        }
    }

    companion object {
        private val kafka: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"))
        private lateinit var kafkaServer: KafkaServer

        @JvmStatic
        @BeforeClass
        fun setup() {
            kafka.start()
            kafkaServer = KafkaServer { bootstrapUrl = kafka.bootstrapServers }
        }
    }
}
