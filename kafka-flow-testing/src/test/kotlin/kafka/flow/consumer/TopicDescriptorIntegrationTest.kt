package kafka.flow.consumer

import kafka.flow.consumer.with.group.id.ignoreTombstones
import kafka.flow.consumer.with.group.id.transformValueToOutput
import kafka.flow.consumer.with.group.id.values
import kafka.flow.consumer.with.group.id.writeOutputToKafkaAndCommit
import kafka.flow.producer.KafkaOutput
import kafka.flow.testing.Await
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestTopicDescriptor
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import org.junit.Test
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils
import strikt.api.expectThat
import strikt.assertions.isEqualTo

class TopicDescriptorIntegrationTest : KafkaServerIntegrationTest() {
    @Test
    fun sendTestRecord_ReceiveTestRecord() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        kafkaServer.admin().createTopics(testTopic1, testTopic2)

        launch {
            kafkaServer.from(testTopic1).consumer()
                .withGroupId(RandomStringUtils.random(5))
                .autoOffsetResetEarliest()
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .transformValueToOutput { KafkaOutput.forValue(kafkaServer, testTopic2, it) }
                .writeOutputToKafkaAndCommit()
        }

        var record: TestObject? = null

        launch {
            kafkaServer.from(testTopic2).consumer()
                .withGroupId(RandomStringUtils.random(5))
                .autoOffsetResetEarliest()
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .values()
                .collect { record = it }
        }
        val expected = TestObject.random()

        kafkaServer.on(testTopic1).send(expected)

        Await().untilAsserted {
            expectThat(record).isEqualTo(expected)
        }
    }
}