package kafka.flow.consumer

import be.delta.flow.time.second
import be.delta.flow.time.seconds
import java.time.Instant
import kafka.flow.producer.KafkaOutput
import kafka.flow.testing.Await
import kafka.flow.testing.TestObject
import kafka.flow.testing.TestServer
import kafka.flow.testing.TestTopicDescriptor
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils.random
import strikt.api.expectThat
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo

@FlowPreview
class DebouncerIntegrationTest : KafkaServerIntegrationTest() {
    @Test
    fun sendRecordThenModifiedRecord_ReceiveModifiedRecord() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2)

        launch {
            TestServer.from(testTopic1)
                .consumer()
                .withGroupId(random(5))
                .autoOffsetResetEarliest()
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .debounceInputOnKey({ _, _ -> Instant.now() + 2.seconds() }, 2.seconds())
                .mapValueToOutput { _, value -> KafkaOutput.forValue(TestServer, testTopic2, value) }
                .writeOutputToKafkaAndCommit()
        }

        val original = TestObject.random()
        val modified = original.copy(value = random(5))
        TestServer.on(testTopic1).send(original)
        TestServer.on(testTopic1).send(modified)

        TestServer.topicTester(testTopic2).expectMessage {
            condition("same record") { expectThat(it).isEqualTo(modified) }
        }
    }

    @Test
    fun sendRecordThenModifiedRecordWithDifferentKeys_ReceiveModifiedRecord() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2)

        launch {
            TestServer.from(testTopic1)
                .consumer()
                .withGroupId(random(5))
                .autoOffsetResetEarliest()
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .debounceInputOnKey({ _, _ -> Instant.now() + 2.seconds() }, 2.seconds())
                .mapValueToOutput { _, value -> KafkaOutput.forValue(TestServer, testTopic2, value) }
                .writeOutputToKafkaAndCommit()
        }

        val original1 = TestObject.random()
        val modified1 = original1.copy(value = random(5))

        TestServer.on(testTopic1).send(original1)
        TestServer.on(testTopic1).send(modified1)

        val original2 = TestObject.random()
        val modified2 = original2.copy(value = random(5))

        TestServer.on(testTopic1).send(original2)
        TestServer.on(testTopic1).send(modified2)

        val records = TestServer
            .from(testTopic2)
            .consumer()
            .withoutGroupId()
            .startFromEarliest()
            .consumerUntilSpecifiedTime(Instant.now() + 3.seconds())
            .readAllPartitions()
            .startConsuming()
            .toList()
            .mapNotNull { if (it is Record) it.value!! else null }

        expectThat(records).containsExactlyInAnyOrder(modified1, modified2)
    }

    @Test
    fun debounceOutput() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2)

        launch {
            TestServer.from(testTopic1)
                .consumer()
                .withGroupId(random(5))
                .autoOffsetResetEarliest()
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .mapValueToOutput { _, value -> KafkaOutput.forValue(TestServer, testTopic2, value) }
                .debounceOutputOnKey({ _, _ -> Instant.now() + 2.seconds() }, 2.seconds())
                .writeOutputToKafkaAndCommit()
        }

        val original1 = TestObject.random()
        val modified1 = original1.copy(value = random(5))

        TestServer.on(testTopic1).send(original1)
        TestServer.on(testTopic1).send(modified1)

        val original2 = TestObject.random()
        val modified2 = original2.copy(value = random(5))

        TestServer.on(testTopic1).send(original2)
        TestServer.on(testTopic1).send(modified2)

        val records = TestServer
            .from(testTopic2)
            .consumer()
            .withoutGroupId()
            .startFromEarliest()
            .consumerUntilSpecifiedTime(Instant.now() + 3.seconds())
            .readAllPartitions()
            .startConsuming()
            .toList()
            .mapNotNull { if (it is Record) it.value!! else null }

        expectThat(records).containsExactlyInAnyOrder(modified1, modified2)
    }

    @Test
    fun debounceOutputWithMultipleMessages() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2)
        val groupId = random(5)

        launch {
            TestServer.from(testTopic1)
                .consumer()
                .withGroupId(groupId)
                .autoOffsetResetEarliest(commitInterval = 1.second())
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .mapValueToOutput { _, value -> KafkaOutput.forValue(TestServer, testTopic2, value) + KafkaOutput.forValue(TestServer, testTopic2, value) }
                .debounceOutputOnKey({ _, _ -> Instant.now() + 2.seconds() }, 2.seconds())
                .writeOutputToKafkaAndCommit()
        }

        val original1 = TestObject.random()
        val modified1 = original1.copy(value = random(5))

        TestServer.on(testTopic1).send(original1)
        TestServer.on(testTopic1).send(modified1)

        val original2 = TestObject.random()
        val modified2 = original2.copy(value = random(5))

        TestServer.on(testTopic1).send(original2)
        TestServer.on(testTopic1).send(modified2)

        val records = TestServer
            .from(testTopic2)
            .consumer()
            .withoutGroupId()
            .startFromEarliest()
            .consumerUntilSpecifiedTime(Instant.now() + 3.seconds())
            .readAllPartitions()
            .startConsuming()
            .toList()
            .mapNotNull { if (it is Record) it.value!! else null }

        expectThat(records).containsExactlyInAnyOrder(modified1, modified2)
    }

    @Test
    fun debounceOutputWithMultiOutputTopics() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        val testTopic3 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2, testTopic3)
        val groupId = random(5)

        launch {
            TestServer.from(testTopic1)
                .consumer()
                .withGroupId(groupId)
                .autoOffsetResetEarliest(commitInterval = 1.second())
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .mapValueToOutput { _, value -> KafkaOutput.forValue(TestServer, testTopic2, value) + KafkaOutput.forValue(TestServer, testTopic3, value) }
                .debounceOutputOnKey({ _, _ -> Instant.now() + 2.seconds() }, 2.seconds())
                .writeOutputToKafkaAndCommit()
        }

        val original1 = TestObject.random()
        val modified1 = original1.copy(value = random(5))

        TestServer.on(testTopic1).send(original1)
        TestServer.on(testTopic1).send(modified1)

        val original2 = TestObject.random()
        val modified2 = original2.copy(value = random(5))

        TestServer.on(testTopic1).send(original2)
        TestServer.on(testTopic1).send(modified2)

        val recordsTopic2 = async {
            TestServer
                .from(testTopic2)
                .consumer()
                .withoutGroupId()
                .startFromEarliest()
                .consumerUntilSpecifiedTime(Instant.now() + 3.seconds())
                .readAllPartitions()
                .startConsuming()
                .toList()
                .mapNotNull { if (it is Record) it.value!! else null }
        }
        val recordsTopic3 = async {
            TestServer
                .from(testTopic3)
                .consumer()
                .withoutGroupId()
                .startFromEarliest()
                .consumerUntilSpecifiedTime(Instant.now() + 3.seconds())
                .readAllPartitions()
                .startConsuming()
                .toList()
                .mapNotNull { if (it is Record) it.value!! else null }
        }

        expectThat(recordsTopic2.await()).containsExactlyInAnyOrder(modified1, modified2)
        expectThat(recordsTopic3.await()).containsExactlyInAnyOrder(modified1, modified2)
    }

    @Test
    fun debounceOutputWithMultipleMessagesInputCommitted() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2)
        val groupId = random(5)

        launch {
            TestServer.from(testTopic1)
                .consumer()
                .withGroupId(groupId)
                .autoOffsetResetEarliest(commitInterval = 1.second())
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .mapValueToOutput { _, value -> KafkaOutput.forValue(TestServer, testTopic2, value) + KafkaOutput.forValue(TestServer, testTopic2, value) }
                .debounceOutputOnKey({ _, _ -> Instant.now() + 2.seconds() }, 2.seconds())
                .writeOutputToKafkaAndCommit()
        }

        val original1 = TestObject.random()
        val modified1 = original1.copy(value = random(5))

        TestServer.on(testTopic1).send(original1)
        TestServer.on(testTopic1).send(modified1)

        val original2 = TestObject.random()
        val modified2 = original2.copy(value = random(5))

        TestServer.on(testTopic1).send(original2)
        TestServer.on(testTopic1).send(modified2)

        Await().untilAsserted {
            TestServer.admin().withAdminClient {
                validateCommittedOffset(this, groupId, testTopic1, 4)
            }
        }
    }

    @Test
    fun debounceInputWithMultiInputTopics() = runTest {
        val testTopic1 = TestTopicDescriptor.next()
        val testTopic2 = TestTopicDescriptor.next()
        val testTopic3 = TestTopicDescriptor.next()
        TestServer.admin().createTopics(testTopic1, testTopic2, testTopic3)
        val groupId = random(5)

        launch {
            TestServer.from(testTopic1, testTopic2)
                .consumer()
                .withGroupId(groupId)
                .autoOffsetResetEarliest(commitInterval = 1.second())
                .consumeUntilStopped()
                .startConsuming()
                .ignoreTombstones()
                .onEachRecord { println("IN ${it.value}") }
                .debounceInputOnKey({ _, _ -> Instant.now() + 2.seconds() }, 2.seconds())
                .onEachRecord { println("OUT ${it.value}") }
                .mapValueToOutput { _, value -> KafkaOutput.forValue(TestServer, testTopic3, value) }
                .writeOutputToKafkaAndCommit()
        }

        val original1 = TestObject.random()
        val modified1 = original1.copy(value = random(5))

        TestServer.on(testTopic1).send(original1)
        TestServer.on(testTopic1).send(modified1)
        TestServer.on(testTopic2).send(original1)
        TestServer.on(testTopic2).send(modified1)

        val recordsTopic3 = async {
            TestServer
                .from(testTopic3)
                .consumer()
                .withoutGroupId()
                .startFromEarliest()
                .consumerUntilSpecifiedTime(Instant.now() + 3.seconds())
                .readAllPartitions()
                .startConsuming()
                .toList()
                .mapNotNull { if (it is Record) it.value!! else null }
        }

        print(recordsTopic3.await())

        expectThat(recordsTopic3.await()).hasSize(2)
        expectThat(recordsTopic3.await()[0]).isEqualTo(modified1)
        expectThat(recordsTopic3.await()[1]).isEqualTo(modified1)
    }

    private fun validateCommittedOffset(admin: AdminClient, groupId: String, topic: TestTopicDescriptor, numberOfMessagesCommitted: Long) {
        val committedOffsets: Map<TopicPartition, Long> = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get().mapValues { (_, value) -> value.offset() }
        val endOffsetRequest = (1..topic.partitionNumber).map { it - 1 }.map { TopicPartition(topic.name, it) }.associateWith { OffsetSpec.latest() }
        val endOffsets: Map<TopicPartition, Long> = admin.listOffsets(endOffsetRequest).all().get().mapValues { (_, value) -> value.offset() }
        endOffsets.forEach { (topicPartition, offset) ->
            expectThat(committedOffsets[topicPartition] ?: 0).describedAs(topicPartition.toString()).isEqualTo(offset)
        }
        expectThat(committedOffsets.values.sum()).isEqualTo(numberOfMessagesCommitted)
    }
}