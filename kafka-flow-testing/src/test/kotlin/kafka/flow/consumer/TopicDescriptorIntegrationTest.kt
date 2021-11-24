package kafka.flow.consumer

import be.delta.flow.time.minute
import be.delta.flow.time.seconds
import kafka.flow.producer.KafkaOutput
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestServer
import kafka.flow.testing.TestTopicDescriptor
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.Test
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class TopicDescriptorIntegrationTest {
    @Test
    fun sendTestRecord_ReceiveTestRecord() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2)

        launch {
            TestServer.from(testTopic1)
                .consumer()
                .withGroupId(RandomStringUtils.random(5))
                .autoOffsetResetEarliest()
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .mapValueToOutput { _, value -> KafkaOutput.forValue(TestServer, testTopic2, value) }
                .writeOutputToKafkaAndCommit()
        }

        val expected = TestObject.random()
        TestServer.on(testTopic1).send(expected)

        TestServer.topicTester(testTopic2).expectMessage {
            condition("same record") { expectThat(it).isEqualTo(expected) }
        }
    }

    @Test
    fun sendTestRecordThenBatch_ReceiveTestRecord() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2)

        val consumer = TestServer.from(testTopic1)
            .consumer()
            .withGroupId(RandomStringUtils.random(5))
            .autoOffsetResetEarliest()
            .consumeUntilStopped()

        launch {
            consumer
                .startConsuming()
                .ignoreTombstones()
                .batchRecords(100, 5.seconds())
                .mapValuesToOutput { keyValues -> KafkaOutput.forValues(TestServer, testTopic2, keyValues.map { it.second }) }
                .writeOutputToKafkaAndCommit()
        }

        val expected = TestObject.random()
        TestServer.on(testTopic1).send(expected)

        TestServer.topicTester(testTopic2).expectMessage {
            condition("same record") { expectThat(it).isEqualTo(expected) }
        }
    }


    @Test
    fun sendTestRecordThenBatchSendManyRecords_ReceiveTestRecord() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2)

        launch {
            TestServer.from(testTopic1)
                .consumer()
                .withGroupId(RandomStringUtils.random(5))
                .autoOffsetResetEarliest()
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .batchRecords(100, 1.minute())
                .mapValuesToOutput { keyValues -> KafkaOutput.forValues(TestServer, testTopic2, keyValues.map { it.second }) }
                .writeOutputToKafkaAndCommit()
        }

        val expected = TestObject.random()
        TestServer.on(testTopic1).send(expected)
        repeat(100) { TestServer.on(testTopic1).send(TestObject.random()) }

        TestServer.topicTester(testTopic2).expectMessage {
            condition("same record") { expectThat(it).isEqualTo(expected) }
        }
    }
}