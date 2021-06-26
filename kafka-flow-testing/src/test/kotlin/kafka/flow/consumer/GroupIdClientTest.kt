package kafka.flow.consumer

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.with.group.id.KafkaFlowConsumerWithGroupId
import kafka.flow.producer.KafkaFlowTopicProducer
import kafka.flow.server.Server
import kafka.flow.testing.Await
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestTopicDescriptor
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEmpty
import java.util.*


class GroupIdClientTest {
    private lateinit var topic: TopicDescriptor<TestObject.Key, String, TestObject>
    private lateinit var producer: KafkaFlowTopicProducer<TestObject.Key, String, TestObject>
    private var consumer: KafkaFlowConsumerWithGroupId? = null

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
    fun subscribeFromBeginning_readAllMessages(): Unit = runTest {
        consumer = KafkaFlowConsumerWithGroupId(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())
        var count = 0
        launch {
            consumer!!
                .startConsuming()
                .deserializeValue { String(it) }
                .values()
                .collect { count++ }
        }

        repeat(10) { producer.send(TestObject.random()) }

        Await().untilAsserted {
            expectThat(count).describedAs("count").isEqualTo(10)
        }
    }


    @Test
    fun subscribeFromTheEnd_readNoMessages(): Unit = runTest {
        repeat(10) { producer.send(TestObject.random()) }
        delay(1000)

        consumer = KafkaFlowConsumerWithGroupId(properties(), listOf(topic.name), StartOffsetPolicy.latest(), AutoStopPolicy.whenUpToDate())
        val count = consumer!!.startConsuming().values().count()

        expectThat(count).describedAs("count").isEqualTo(0)
    }

    @Test
    fun stopReading_closeConsumer(): Unit = runTest {
        repeat(10) { producer.send(TestObject.random()) }

        consumer = KafkaFlowConsumerWithGroupId(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())

        var count = 0
        launch {
            count = consumer!!.startConsuming().values()
                .take(3)
                .count()
        }

        Await().untilAsserted {
            expectThat(consumer!!).get("isRunning") { isRunning }.isEqualTo(false)
            expectThat(count).describedAs("count").isEqualTo(3)
        }
        consumer!!.stop()
    }

    @Test
    fun sendBatches_continueReceiving(): Unit = runTest {
        consumer = KafkaFlowConsumerWithGroupId(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())
        var count = 0
        launch {
            consumer!!.startConsuming().values().collect { count++ }
        }

        repeat(10) { producer.send(TestObject.random()) }

        Await().untilAsserted {
            expectThat(count).describedAs("count").isEqualTo(10)
        }

        repeat(10) { producer.send(TestObject.random()) }

        Await().untilAsserted {
            expectThat(count).describedAs("count").isEqualTo(20)
        }
    }

    @Test
    fun multiplesConsumers_AssignmentSpread() = runBlocking {
        val consumer1 = KafkaFlowConsumerWithGroupId(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())
        val consumer2 = KafkaFlowConsumerWithGroupId(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())
        try {
            var assignment1: List<TopicPartition> = emptyList()
            var assignment2: List<TopicPartition> = emptyList()

            launch {
                consumer1.startConsuming().filterIsInstance<PartitionChangedMessage<Unit, Unit, Unit, Unit>>()
                    .onEach { println("assignment1 ${it.newAssignment}") }
                    .collect { assignment1 = it.newAssignment }
            }

            Await().untilAsserted {
                expectThat(assignment1).hasSize(topic.partitionNumber)
                (1..topic.partitionNumber).map { it - 1 }.forEach {
                    expectThat(assignment1).contains(TopicPartition(topic.name, it))
                }
            }

            launch {
                consumer2.startConsuming().filterIsInstance<PartitionChangedMessage<Unit, Unit, Unit, Unit>>()
                    .onEach { println("assignment2 ${it.newAssignment}") }
                    .collect { assignment2 = it.newAssignment }
            }

            Await().untilAsserted {
                val assignment = assignment1 + assignment2
                expectThat(assignment1).isNotEmpty()
                expectThat(assignment2).isNotEmpty()
                expectThat(assignment).hasSize(topic.partitionNumber)
                (1..topic.partitionNumber).map { it - 1 }.forEach {
                    expectThat(assignment).contains(TopicPartition(topic.name, it))
                }
            }
        } finally {
            consumer1.stop()
            consumer2.stop()
        }
    }

    @Test
    fun sendTestObject_expectSameObject() = runTest {
        consumer = KafkaFlowConsumerWithGroupId(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())

        var record: TestObject? = null
        launch {
            consumer!!.startConsuming().deserializeValue { topic.deserializeValue(it) }.values().collect { record = it }
        }

        val expected = TestObject.random()
        producer.send(expected)

        Await().untilAsserted {
            expectThat(record).isEqualTo(expected)
        }
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
        private lateinit var server: Server

        @JvmStatic
        @BeforeClass
        fun setup() {
            kafka.start()
            server = Server { bootstrapUrl = kafka.bootstrapServers }
        }
    }
}
