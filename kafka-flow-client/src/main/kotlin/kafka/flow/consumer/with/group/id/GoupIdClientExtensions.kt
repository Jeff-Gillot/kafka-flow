@file:Suppress("UNCHECKED_CAST")

package kafka.flow.consumer.with.group.id

import be.delta.flow.time.seconds
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.*
import kafka.flow.consumer.without.group.id.onStartConsuming
import kafka.flow.producer.KafkaOutput
import kafka.flow.producer.TopicDescriptorRecord
import kafka.flow.server.KafkaServer
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.apache.kafka.common.TopicPartition
import java.time.Duration

public suspend fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output>>.createTransactions(
    maxOpenTransactions: Int = 1024,
    commitInterval: Duration = 30.seconds()
): Flow<KafkaMessageWithTransaction<Key, Partition, Value, Output>> {
    val transactionManager = TransactionManager(maxOpenTransactions)
    var client: KafkaFlowConsumerWithGroupId<*>? = null
    var commitLoop: Job? = null
    return this
        .onCompletion {
            commitLoop?.cancel()
            client?.let { transactionManager.rollbackAndCommit(it) }
        }
        .onStartConsuming {
            require(it.client is KafkaFlowConsumerWithGroupId) { "You can only create transactions on a groupId client" }
            client = it.client
            commitLoop = CoroutineScope(currentCoroutineContext()).launch {
                while (true) {
                    delay(commitInterval.toMillis())
                    transactionManager.rollbackAndCommit(it.client)
                }
            }
        }
        .map { message ->
            if (message is Record) {
                val transaction = Transaction(TopicPartition(message.consumerRecord.topic(), message.consumerRecord.partition()), message.consumerRecord.offset(), transactionManager)
                transaction.lock()
                RecordWithTransaction(message, transaction)
            } else {
                message as KafkaMessageWithTransaction<Key, Partition, Value, Output>
            }
        }
}

public fun <Key, Partition, Value, Output> Flow<KafkaMessageWithTransaction<Key, Partition, Value, Output>>.onEachRecord(block: suspend (RecordWithTransaction<Key, Partition, Value, Output>) -> Unit): Flow<KafkaMessageWithTransaction<Key, Partition, Value, Output>> {
    return onEach { message ->
        if (message is RecordWithTransaction)
            block.invoke(message)
    }
}

public fun <Key, Partition, Value, Output> Flow<KafkaMessageWithTransaction<Key, Partition, Value, Output>>.values(): Flow<Value> {
    return filterIsInstance<RecordWithTransaction<Key, Partition, Value, Output>>()
        .map { it.value }
}

public fun <Key, Partition, Value, Output> Flow<KafkaMessageWithTransaction<Key, Partition, Value, Unit>>.mapValueToOutput(block: suspend (Value) -> Output):
        Flow<KafkaMessageWithTransaction<Key, Partition, Value, Output>> {
    return map { message ->
        if (message is RecordWithTransaction) {
            RecordWithTransaction(
                message.consumerRecord,
                message.key,
                message.partitionKey,
                message.value,
                block.invoke(message.value),
                message.transaction
            )
        } else {
            message as KafkaMessageWithTransaction<Key, Partition, Value, Output>
        }
    }
}

public suspend fun <Key, Partition, Value> Flow<KafkaMessageWithTransaction<Key, Partition, Value, KafkaOutput>>.writeOutputToKafkaAndCommit() {
    collect { message ->
        if (message is RecordWithTransaction) {
            val outputs = message.output
            outputs.records.forEach { outputRecord ->
                val output: TopicDescriptorRecord<Any, Any, Any> = outputRecord as TopicDescriptorRecord<Any, Any, Any>
                message.transaction.lock()
                when (output) {
                    is TopicDescriptorRecord.Record -> outputRecord.kafkaServer.on(output.topicDescriptor).send(output.value, message.transaction)
                    is TopicDescriptorRecord.Tombstone -> outputRecord.kafkaServer.on(output.topicDescriptor).sendTombstone(output.key, output.timestamp, message.transaction)
                }
            }
            message.transaction.unlock()
        }
    }
}

public fun <Key, Partition, Value, Output> Flow<KafkaMessageWithTransaction<Key, Partition, Value?, Output>>.ignoreTombstones(): Flow<KafkaMessageWithTransaction<Key, Partition, Value, Output>> {
    return filter { message ->
        if (message is RecordWithTransaction) {
            message.value != null
        } else {
            true
        }
    } as Flow<KafkaMessageWithTransaction<Key, Partition, Value, Output>>
}
