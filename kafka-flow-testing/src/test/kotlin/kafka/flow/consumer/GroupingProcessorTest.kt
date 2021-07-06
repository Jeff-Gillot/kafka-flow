package kafka.flow.consumer

import be.delta.flow.time.seconds
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestServer
import kafka.flow.testing.TestTopicDescriptor
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.launch
import org.junit.Test
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils


internal class GroupingProcessorTest : KafkaServerIntegrationTest() {
    @Test
    fun testGrouping() = runTest {
        val testTopic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic)

        launch {
            TestServer.from(testTopic)
                .consumer()
                .withGroupId(RandomStringUtils.random(5))
                .autoOffsetResetEarliest()
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .groupByPartitionKey(testTopic, 60.seconds(), 10) { flow, partitionKey ->
                    println("New processor $partitionKey")
                    flow
                        .onEachRecord { delay(1000) }
                        .onEachRecord { println("$partitionKey - ${it.value}") }
                        .onStopConsuming { println("StopConsuming $partitionKey") }
                        .onCompletion { println("Finished $partitionKey") }
                        .collect()
                }
        }

        TestServer.on(testTopic).send(TestObject.random().copy(key = TestObject.Key("1", "A")))
        TestServer.on(testTopic).send(TestObject.random().copy(key = TestObject.Key("2", "A")))
        TestServer.on(testTopic).send(TestObject.random().copy(key = TestObject.Key("3", "A")))
        TestServer.on(testTopic).send(TestObject.random().copy(key = TestObject.Key("1", "B")))
        TestServer.on(testTopic).send(TestObject.random().copy(key = TestObject.Key("2", "B")))
        TestServer.on(testTopic).send(TestObject.random().copy(key = TestObject.Key("4", "A")))

        delay(15000)
        TestServer.on(testTopic).send(TestObject.random().copy(key = TestObject.Key("5", "A")))


        delay(30000)
        currentCoroutineContext().cancelChildren()
    }
}