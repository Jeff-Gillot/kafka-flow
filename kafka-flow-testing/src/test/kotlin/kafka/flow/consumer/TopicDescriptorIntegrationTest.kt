package kafka.flow.consumer

import kafka.flow.producer.KafkaOutput
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestServer
import kafka.flow.testing.TestTopicDescriptor
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
        TestServer.admin().createTopics(testTopic1, testTopic2)

        launch {
            TestServer.from(testTopic1)
                .consumer()
                .withGroupId(RandomStringUtils.random(5))
                .autoOffsetResetEarliest()
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .mapValueToOutput { KafkaOutput.forValue(TestServer, testTopic2, it) }
                .writeOutputToKafkaAndCommit()
        }

        val expected = TestObject.random()
        TestServer.on(testTopic1).send(expected)

        TestServer.topicTester(testTopic2).expectMessage {
            condition("same record") { expectThat(it).isEqualTo(expected) }
        }
    }
}