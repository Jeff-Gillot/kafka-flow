package kafka.flow.testing

import be.delta.flow.time.minutes
import be.delta.flow.time.seconds
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.*
import kafka.flow.server.KafkaServer
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.toList
import java.time.Duration

public class TopicTester<Key, PartitionKey, Value>(private val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>, private val kafkaServer: KafkaServer) {
    public suspend fun expectMessage(
        timeout: Duration = 10.seconds(),
        startOffsetPolicy: StartOffsetPolicy = StartOffsetPolicy.specificOffsetFromNow(5.minutes()),
        filter: suspend (Value) -> Boolean = { true },
        conditionBuilder: ConditionsBuilder<Value>.() -> Unit
    ) {
        val conditions = ConditionsBuilder<Value>().apply(conditionBuilder).build()
        val (record, records) = findMatchingRecords(startOffsetPolicy, timeout, filter, conditions)
        handleResult(conditions, records, record)
    }

    public suspend fun expectNoMessages(
        timeout: Duration = 2.seconds(),
        startOffsetPolicy: StartOffsetPolicy = StartOffsetPolicy.specificOffsetFromNow(5.minutes()),
        filter: suspend (Value) -> Boolean = { true },
        conditionBuilder: ConditionsBuilder<Value>.() -> Unit
    ) {
        val conditions = ConditionsBuilder<Value>().apply(conditionBuilder).build()
        val (record, records) = findMatchingRecords(startOffsetPolicy, timeout, filter, conditions)
        handleNoMessageResult(conditions, records, record)
    }

    public suspend fun expectTombstone(
        timeout: Duration = 10.seconds(),
        startOffsetPolicy: StartOffsetPolicy = StartOffsetPolicy.specificOffsetFromNow(5.minutes()),
        conditionBuilder: ConditionsBuilder<Key>.() -> Unit
    ) {
        val conditions = ConditionsBuilder<Key>().apply(conditionBuilder).build()
        var key: Key? = null
        val keyValues = kafkaServer.from(topicDescriptor)
            .consumer()
            .withoutGroupId()
            .startFromPolicy(startOffsetPolicy)
            .consumerUntilSpecifiedOffsetFromNow(timeout)
            .readAllPartitions()
            .startConsuming()
            .onEachRecord { if (conditions.test(it.key).isSuccess && it.value == null) key = it.key }
            .takeWhile { key == null }
            .toList()
            .filterIsInstance<Record<Key, *, Value?, *, *>>()
            .map { Pair(it.key, it.value) }
            .toList()
        handleTombstoneResult(conditions, keyValues, key)
    }

    private suspend fun findMatchingRecords(
        startOffsetPolicy: StartOffsetPolicy,
        timeout: Duration,
        filter: suspend (Value) -> Boolean,
        conditions: Conditions<Value>,
    ): Pair<Value?, List<Value>> {
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
        return Pair(record, records)
    }

    private fun handleTombstoneResult(
        conditions: Conditions<Key>,
        records: List<Pair<Key, Value?>>,
        validRecord: Key?
    ) {
        if (validRecord != null) {
            val text = StringBuilder()
            text.appendLine("Found tombstone matching the following conditions on ${topicDescriptor.name}")
            text.appendLine(conditions.descriptionFor(validRecord))
            println(text)
        } else if (records.isEmpty()) {
            val text = StringBuilder()
            text.appendLine("Did not found any record on ${topicDescriptor.name}")
            text.appendLine(conditions.description())
            throw AssertionError(text)
        } else {
            val text = StringBuilder()
            text.appendLine("Did not found any tombstone matching the following conditions on ${topicDescriptor.name}")
            text.appendLine(conditions.description())
            text.appendLine("Here is the list of keys found on the topic that do not match the conditions")
            for ((key, value) in records) {
                text.appendLine(conditions.descriptionFor(key))
                when (value) {
                    null -> text.appendLine("    OK   -> isTombstone")
                    else -> text.appendLine("    FAIL -> isTombstone ($value)")
                }
            }
            throw AssertionError(text)
        }
    }

    private fun handleNoMessageResult(
        conditions: Conditions<Value>,
        records: List<Value>,
        validRecord: Value?
    ) {
        if (validRecord != null) {
            val text = StringBuilder()
            text.appendLine("Found record matching the following conditions on ${topicDescriptor.name} but expected none")
            text.appendLine(conditions.descriptionFor(validRecord))
            throw AssertionError(text)
        } else if (records.isEmpty()) {
            val text = StringBuilder()
            text.appendLine("Did not found any record on ${topicDescriptor.name}")
            text.appendLine(conditions.description())
        } else {
            val text = StringBuilder()
            text.appendLine("Did not found any record matching the following conditions on ${topicDescriptor.name}, there where ${records.size} that did not match the conditions")
            text.appendLine(conditions.description())
        }
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