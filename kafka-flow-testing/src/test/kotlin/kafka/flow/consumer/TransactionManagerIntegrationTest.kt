package kafka.flow.consumer

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.with.group.id.KafkaFlowConsumerWithGroupId
import kafka.flow.consumer.with.group.id.createTransactions
import kafka.flow.producer.KafkaFlowTopicProducer
import kafka.flow.server.Server
import kafka.flow.testing.Await
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestTopicDescriptor
import kafka.flow.utils.second
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.util.*


class TransactionManagerIntegrationTest {
    private lateinit var topic: TopicDescriptor<TestObject.Key, String, TestObject>
    private lateinit var producer: KafkaFlowTopicProducer<TestObject.Key, String, TestObject>
    private var consumer: KafkaFlowConsumerWithGroupId? = null
    var groupId = "test-client-${UUID.randomUUID()}"
    val admin = AdminClient.create(properties())

    @Before
    fun createTestTopic() {
        topic = TestTopicDescriptor.next()
        producer = server.on(topic)
        admin.createTopics(listOf(NewTopic(topic.name, 12, 1))).all().get()
        groupId = "test-client-${UUID.randomUUID()}"
    }

    @After
    fun after() {
        producer.close()
    }

    @Test
    fun committingTransaction_changesCommittedOffsets(): Unit = runTest {
        consumer = KafkaFlowConsumerWithGroupId(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())

        launch {
            consumer!!.startConsuming()
                .createTransactions(10, 1.second()).onEachRecord { it.transaction?.unlock() }.collect()
        }

        repeat(10) { producer.send(TestObject.random()) }

        Await().untilAsserted {
            validateCommittedOffset(10)
        }
    }

    @Test
    fun committingTransactionOnStopConsumer_changesCommittedOffsets(): Unit = runTest {
        consumer = KafkaFlowConsumerWithGroupId(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())

        launch {
            consumer!!.startConsuming()
                .createTransactions(10, 1.second()).onEachRecord { it.transaction?.unlock() }.values().take(10).collect()
        }

        repeat(20) { producer.send(TestObject.random()) }

        Await().untilAsserted {
            val committedOffsets: Map<TopicPartition, Long> = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get().mapValues { (_, value) -> value.offset() }
            expectThat(committedOffsets.values.sum()).isEqualTo(10)
        }
    }

    private fun validateCommittedOffset(numberOfMessagesCommitted: Long) {
        val committedOffsets: Map<TopicPartition, Long> = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get().mapValues { (_, value) -> value.offset() }
        val endOffsetRequest = (1..topic.partitionNumber).map { it - 1 }.map { TopicPartition(topic.name, it) }.associateWith { OffsetSpec.latest() }
        val endOffsets: Map<TopicPartition, Long> = admin.listOffsets(endOffsetRequest).all().get().mapValues { (_, value) -> value.offset() }
        endOffsets.forEach { (topicPartition, offset) ->
            expectThat(committedOffsets[topicPartition] ?: 0).describedAs(topicPartition.toString()).isEqualTo(offset)
        }
        expectThat(committedOffsets.values.sum()).isEqualTo(numberOfMessagesCommitted)
    }


    private fun properties(): Properties {
        val properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        properties[ConsumerConfig.GROUP_ID_CONFIG] = groupId
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
