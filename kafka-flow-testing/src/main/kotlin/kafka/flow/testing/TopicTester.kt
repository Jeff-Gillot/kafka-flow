package kafka.flow.testing

import be.delta.flow.time.minutes
import be.delta.flow.time.seconds
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.StartOffsetPolicy
import kafka.flow.consumer.ignoreTombstones
import kafka.flow.consumer.values
import kafka.flow.server.KafkaServer
import kotlinx.coroutines.flow.*
import java.time.Duration

public class TopicTester<Key, PartitionKey, Value>(private val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>, private val kafkaServer: KafkaServer) {
    public suspend fun expectMessage(
        timeout: Duration = 10.seconds(),
        startOffsetPolicy: StartOffsetPolicy = StartOffsetPolicy.specificOffsetFromNow(5.minutes()),
        filter: suspend (Value) -> Boolean = { true },
        conditionBuilder: ConditionsBuilder<Value>.() -> Unit
    ) {
        val conditions = ConditionsBuilder<Value>().apply(conditionBuilder).build()

        var record: Value? = null
        val records = kafkaServer.from(topicDescriptor)
            .consumer()
            .withoutGroupId()
            .startFromPolicy(startOffsetPolicy)
            .consumerUntilSpecifiedOffsetFromNow(timeout)
            .readAllPartitions()
            .startConsuming()
            .ignoreTombstones()
            .values()
            .filter(filter)
            .onEach { if (conditions.test(it).isSuccess) record = it }
            .takeWhile { record == null }
            .toList()

        handleResult(conditions, records, record)
    }

    private fun handleResult(
        conditions: Conditions<Value>,
        records: List<Value>,
        validRecord: Value?
    ) {
        if (validRecord != null) {
            val text = StringBuilder()
            text.appendLine("Found record matching the following conditions on ${topicDescriptor.name}")
            text.appendLine(conditions.descriptionFor(validRecord))
            println(text)
        } else if (records.isEmpty()) {
            val text = StringBuilder()
            text.appendLine("Did not found any record on ${topicDescriptor.name}")
            text.appendLine(conditions.description())
            throw AssertionError(text)
        } else {
            val text = StringBuilder()
            text.appendLine("Did not found any record matching the following conditions on ${topicDescriptor.name}")
            text.appendLine(conditions.description())
            text.appendLine("Here is the list of records found on the topic that do not match the conditions")
            for (record in records) {
                text.appendLine(conditions.descriptionFor(record))
            }
            throw AssertionError(text)
        }
    }
}