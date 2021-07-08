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
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2)

        launch {
            TestServer.from(testTopic1, testTopic2)
                .consumer()
                .withGroupId(RandomStringUtils.random(5))
                .autoOffsetResetEarliest()
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .groupByPartitionKey(60.seconds(), 10) { flow, partitionKey ->
                    println("New processor $partitionKey")
                    flow
                        .onEachRecord { delay(1000) }
                        .onEachRecord { println("$partitionKey - ${it.value}") }
                        .onStopConsuming { println("StopConsuming $partitionKey") }
                        .onCompletion { println("Finished $partitionKey") }
                        .collect()
                }
        }

        TestServer.on(testTopic1).send(TestObject.random().copy(key = TestObject.Key("1", "A")))
        TestServer.on(testTopic1).send(TestObject.random().copy(key = TestObject.Key("2", "A")))
        TestServer.on(testTopic1).send(TestObject.random().copy(key = TestObject.Key("3", "A")))
        TestServer.on(testTopic1).send(TestObject.random().copy(key = TestObject.Key("1", "B")))
        TestServer.on(testTopic2).send(TestObject.random().copy(key = TestObject.Key("2", "B")))
        TestServer.on(testTopic2).send(TestObject.random().copy(key = TestObject.Key("4", "A")))

        delay(15000)
        TestServer.on(testTopic1).send(TestObject.random().copy(key = TestObject.Key("5", "A")))


        delay(30000)
        currentCoroutineContext().cancelChildren()
    }
}