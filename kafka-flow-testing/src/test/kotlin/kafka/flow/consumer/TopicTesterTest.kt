package kafka.flow.consumer

import be.delta.flow.time.seconds
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestServer
import kafka.flow.testing.TestTopicDescriptor
import org.junit.Test
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import java.time.Instant

internal class TopicTesterTest : KafkaServerIntegrationTest() {
    @Test
    fun testNoMessages() = runTest {
        val topic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(topic)

        val testObject = TestObject.random().copy(value = "value1")
        TestServer.on(topic).send(testObject)

        TestServer.topicTester(topic).expectMessage {
            condition("key == ${testObject.key}") { expectThat(it.key).isEqualTo(testObject.key) }
            condition("value == value1") { expectThat(it.value).isEqualTo("value1") }
        }

        TestServer.topicTester(topic).expectNoMessages {
            condition("key == ${testObject.key}") { expectThat(it.key).isEqualTo(testObject.key) }
            condition("value == value1") { expectThat(it.value).isEqualTo("value2") }
        }
    }

    @Test
    fun testNoMessages_failsIfMessageExists() = runTest {
        val topic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(topic)

        val testObject = TestObject.random()
        TestServer.on(topic).send(testObject)

        expectThrows<AssertionError> {
            TestServer.topicTester(topic).expectNoMessages {
                condition("it == $testObject") { expectThat(it).isEqualTo(testObject) }
            }
        }
    }

    @Test
    fun expectTombstone_succeedIfTombstonePresent() = runTest {
        val topic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(topic)

        val testObject = TestObject.random()
        TestServer.on(topic).sendTombstone(testObject.key, Instant.now())

        TestServer.topicTester(topic).expectTombstone {
            condition("key == ${testObject.key}") { expectThat(it).isEqualTo(testObject.key) }
        }
    }

    @Test
    fun expectTombstone_failsIfTombstoneNotPresent() = runTest {
        val topic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(topic)

        val testObject = TestObject.random()
        TestServer.on(topic).send(testObject)

        expectThrows<AssertionError> {
            TestServer.topicTester(topic).expectTombstone(timeout = 2.seconds()) {
                condition("key == ${testObject.key}") { expectThat(it).isEqualTo(testObject.key) }
            }
        }
    }
}