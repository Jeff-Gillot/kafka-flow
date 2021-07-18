package kafka.flow.consumer

import be.delta.flow.time.day
import be.delta.flow.time.hour
import be.delta.flow.time.second
import be.delta.flow.time.seconds
import kafka.flow.testing.Await
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestServer
import kafka.flow.testing.TestTopicDescriptor
import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import java.time.Instant

class CacheConsumerTest : KafkaServerIntegrationTest() {

    @Test
    fun testNoMessage_emptyCache() = runTest {
        val topic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(topic)

        val cache = TestServer.from(topic).startCacheConsumer()

        expectThat(cache.all()).isEmpty()
    }

    @Test
    fun testSendMessage_cacheHasMessage() = runTest {
        val topic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(topic)

        val cache = TestServer.from(topic).startCacheConsumer()
        val value = TestObject.random()
        TestServer.on(topic).send(value)
        TestServer.on(topic).send(value)

        Await().untilAsserted {
            expectThat(cache.all()).hasSize(1)
            expectThat(cache.get(value.key)).isEqualTo(value)
            expectThat(cache.values()).containsExactlyInAnyOrder(value)
            expectThat(cache.keys()).containsExactlyInAnyOrder(value.key)
        }
    }

    @Test
    fun testSendTwoMessages_cacheHasMessages() = runTest {
        val topic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(topic)

        val cache = TestServer.from(topic).startCacheConsumer()
        val value1 = TestObject.random()
        val value2 = TestObject.random()
        TestServer.on(topic).send(value1)
        TestServer.on(topic).send(value2)

        Await().untilAsserted {
            expectThat(cache.get(value1.key)).isEqualTo(value1)
            expectThat(cache.get(value2.key)).isEqualTo(value2)
            expectThat(cache.all()).hasSize(2)
            expectThat(cache.values()).containsExactlyInAnyOrder(value1, value2)
            expectThat(cache.keys()).containsExactlyInAnyOrder(value1.key, value2.key)
        }
    }

    @Test
    fun testNewMessages_cacheOverwriteSameKey() = runTest {
        val topic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(topic)

        val cache = TestServer.from(topic).startCacheConsumer()
        val value1 = TestObject.random()
        val value2 = value1.copy(value = "newValue")
        TestServer.on(topic).send(value1)
        TestServer.on(topic).send(value2)

        Await().untilAsserted {
            expectThat(cache.get(value2.key)).isEqualTo(value2)
            expectThat(cache.all()).hasSize(1)
            expectThat(cache.values()).containsExactlyInAnyOrder(value2)
            expectThat(cache.keys()).containsExactlyInAnyOrder(value2.key)
        }
    }

    @Test
    fun testOldMessages_ignore() = runTest {
        val topic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(topic)

        val cache = TestServer.from(topic).startCacheConsumer(1.hour())
        val value1 = TestObject.random()
        val value2 = TestObject.random().copy(timestamp = Instant.now() - 1.day())
        TestServer.on(topic).send(value1)
        TestServer.on(topic).send(value2)

        Await().untilAsserted {
            expectThat(cache.get(value1.key)).isEqualTo(value1)
            expectThat(cache.all()).hasSize(1)
            expectThat(cache.values()).containsExactlyInAnyOrder(value1)
            expectThat(cache.keys()).containsExactlyInAnyOrder(value1.key)
        }
    }

    @Test
    fun testOldMessages_discard() = runTest {
        val topic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(topic)

        val cache = TestServer.from(topic).startCacheConsumer(5.seconds(), 1.second())
        val value1 = TestObject.random()
        TestServer.on(topic).send(value1)

        Await().untilAsserted {
            expectThat(cache.get(value1.key)).isEqualTo(value1)
            expectThat(cache.all()).hasSize(1)
            expectThat(cache.values()).containsExactlyInAnyOrder(value1)
            expectThat(cache.keys()).containsExactlyInAnyOrder(value1.key)
        }

        Await().untilAsserted {
            expectThat(cache.get(value1.key)).isEqualTo(null)
            expectThat(cache.all()).hasSize(0)
        }
    }
}