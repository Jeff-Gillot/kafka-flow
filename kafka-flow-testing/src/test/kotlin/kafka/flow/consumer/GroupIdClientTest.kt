package kafka.flow.consumer

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.with.group.id.KafkaFlowConsumerWithGroupId
import kafka.flow.server.Server
import kafka.flow.producer.KafkaFlowTopicProducer
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestTopicDescriptor
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.launch
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
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.util.*


class GroupIdClientTest {
    private lateinit var topic: TopicDescriptor<TestObject.Key, String, TestObject>
    private lateinit var producer: KafkaFlowTopicProducer<TestObject.Key, String, TestObject>

    @Before
    fun createTestTopic() {
        topic = TestTopicDescriptor.next()
        producer = server.on(topic)
        val admin = AdminClient.create(properties())
        admin.createTopics(listOf(NewTopic(topic.name, 12, 1))).all().get()
    }

    @After
    fun after() {
        producer.close()
    }

    @Test
    fun subscribeFromBeginning_readAllMessages(): Unit = runBlocking {
        val consumer = KafkaFlowConsumerWithGroupId(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())
        var count = 0
        launch {
            consumer
                .startConsuming()
                .deserializeValue { String(it) }
                .values()
                .collect { count++ }
        }

        repeat(10) { producer.send(TestObject.random()) }

        Await().untilAsserted {
            expectThat(count).describedAs("count").isEqualTo(10)
        }

        consumer.stop()
    }


    @Test
    fun subscribeFromTheEnd_readNoMessages(): Unit = runBlocking {
        repeat(10) { producer.send(TestObject.random()) }
        delay(1000)

        val consumer = KafkaFlowConsumerWithGroupId(properties(), listOf(topic.name), StartOffsetPolicy.latest(), AutoStopPolicy.whenUpToDate())
        val count = consumer.startConsuming().values().count()

        expectThat(count).describedAs("count").isEqualTo(0)
    }

    private fun properties(): Properties {
        val properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        properties[ConsumerConfig.GROUP_ID_CONFIG] = "test-client"
        return properties
    }

    companion object {
        private val kafka: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"))
        private lateinit var server: Server

        @JvmStatic
        @BeforeClass
        fun setup() {
            kafka.start()
            server = Server { bootstrapUrl = kafka.bootstrapServers }
        }
    }
}
