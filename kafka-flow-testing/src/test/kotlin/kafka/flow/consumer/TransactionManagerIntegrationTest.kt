package kafka.flow.consumer

import be.delta.flow.time.second
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.with.group.id.KafkaFlowConsumerWithGroupIdImpl
import kafka.flow.consumer.with.group.id.createTransactions
import kafka.flow.consumer.with.group.id.onEachRecord
import kafka.flow.consumer.with.group.id.values
import kafka.flow.consumer.without.group.id.deserializeUsing
import kafka.flow.producer.KafkaFlowTopicProducer
import kafka.flow.server.KafkaServer
import kafka.flow.testing.Await
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestTopicDescriptor
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
    private var consumer: KafkaFlowConsumerWithGroupIdImpl? = null
    var groupId = "test-client-${UUID.randomUUID()}"
    val admin = AdminClient.create(properties())

    @Before
    fun createTestTopic() {
        topic = TestTopicDescriptor.next()
        producer = kafkaServer.on(topic)
        admin.createTopics(listOf(NewTopic(topic.name, 12, 1))).all().get()
        groupId = "test-client-${UUID.randomUUID()}"
    }

    @After
    fun after() {
        producer.close()
    }

    @Test
    fun committingTransaction_changesCommittedOffsets(): Unit = runTest {
        consumer = KafkaFlowConsumerWithGroupIdImpl(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())

        launch {
            consumer!!.startConsuming()
                .createTransactions(10, 1.second()).onEachRecord { it.transaction.unlock() }.collect()
        }

        repeat(10) { producer.send(TestObject.random()) }

        Await().untilAsserted {
            validateCommittedOffset(10)
        }
    }

    @Test
    fun committingTransactionOnStopConsumer_changesCommittedOffsets(): Unit = runTest {
        consumer = KafkaFlowConsumerWithGroupIdImpl(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())

        launch {
            consumer!!.startConsuming()
                .createTransactions(10, 1.second()).onEachRecord { it.transaction.unlock() }.values().take(10).collect()
        }

        repeat(20) { producer.send(TestObject.random()) }

        Await().untilAsserted {
            val committedOffsets: Map<TopicPartition, Long> = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get().mapValues { (_, value) -> value.offset() }
            expectThat(committedOffsets.values.sum()).isEqualTo(10)
        }
    }


    @Test
    fun rollbackTransaction_triggersRetry(): Unit = runTest {
        consumer = KafkaFlowConsumerWithGroupIdImpl(properties(), listOf(topic.name), StartOffsetPolicy.earliest(), AutoStopPolicy.never())

        var failedRecord: TestObject? = null
        var successRecord: TestObject? = null
        val expected: TestObject = TestObject.random()

        launch {
            consumer!!.startConsuming()
                .deserializeUsing(topic)
                .createTransactions(10, 1.second()).onEachRecord {
                    if (failedRecord == null) {
                        failedRecord = it.value
                        it.transaction.rollback()
                    } else {
                        successRecord = it.value
                        it.transaction.unlock()
                    }
                }
                .collect()
        }

        producer.send(expected)

        Await().untilAsserted {
            expectThat(failedRecord).isEqualTo(expected)
            expectThat(successRecord).isEqualTo(expected)
            val committedOffsets: Map<TopicPartition, Long> = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get().mapValues { (_, value) -> value.offset() }
            expectThat(committedOffsets.values.sum()).isEqualTo(1)
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
        private lateinit var kafkaServer: KafkaServer

        @JvmStatic
        @BeforeClass
        fun setup() {
            kafka.start()
            kafkaServer = KafkaServer { bootstrapUrl = kafka.bootstrapServers }
        }
    }
}
