package kafka.flow.consumer

import be.delta.flow.time.seconds
import kafka.flow.testing.Await
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestServer
import kafka.flow.testing.TestTopicDescriptor
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.launch
import org.junit.Test
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils
import strikt.api.expectThat
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo


internal class GroupingProcessorTest {
    @Test
    fun testGrouping() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2)

        val results = HashMap<String, ArrayList<TestObject>>()

        launch {
            TestServer.from(testTopic1, testTopic2)
                .consumer()
                .withGroupId(RandomStringUtils.random(5))
                .autoOffsetResetEarliest()
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .groupByPartitionKey(60.seconds(), 10) { flow, partitionKey ->
                    val list = results.computeIfAbsent(partitionKey) { ArrayList() }
                    println("New processor $partitionKey")
                    flow
                        .onEachRecord { delay(100) }
                        .onEachRecord { println("$partitionKey - ${it.value}") }
                        .onEachRecord { list.add(it.value) }
                        .onStopConsuming { println("StopConsuming $partitionKey") }
                        .onCompletion { println("Finished $partitionKey") }
                        .collect()
                }
        }

        val testObject1 = TestObject.random().copy(key = TestObject.Key("1", "A"))
        val testObject2 = TestObject.random().copy(key = TestObject.Key("2", "A"))
        val testObject3 = TestObject.random().copy(key = TestObject.Key("3", "A"))
        val testObject4 = TestObject.random().copy(key = TestObject.Key("1", "B"))
        val testObject5 = TestObject.random().copy(key = TestObject.Key("2", "B"))
        val testObject6 = TestObject.random().copy(key = TestObject.Key("4", "A"))

        TestServer.on(testTopic1).send(testObject1)
        TestServer.on(testTopic1).send(testObject2)
        TestServer.on(testTopic1).send(testObject3)
        TestServer.on(testTopic1).send(testObject4)
        TestServer.on(testTopic2).send(testObject5)
        TestServer.on(testTopic2).send(testObject6)

        //Order isn't guaranteed because the data comes from multiples topics
        Await().untilAsserted() {
            expectThat(results).hasSize(2)
            expectThat(results).get { get("A")!!.toList().sortedBy { it.key.id } }.isEqualTo(listOf(testObject1, testObject2, testObject3, testObject6))
            expectThat(results).get { get("B")!!.toList().sortedBy { it.key.id } }.isEqualTo(listOf(testObject4, testObject5))
        }
    }
}