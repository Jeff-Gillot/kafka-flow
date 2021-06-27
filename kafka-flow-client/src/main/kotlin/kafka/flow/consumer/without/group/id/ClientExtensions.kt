package kafka.flow.consumer

import kafka.flow.TopicDescriptor
import kotlinx.coroutines.flow.*

public suspend fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output>>.collectValues(block: suspend (Value) -> Unit) {
    this.filterIsInstance<Record<Key, Partition, Value, Output>>()
        .collect { block.invoke(it.value) }
}

public suspend fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Unit, Output>>.deserializeValue(block: suspend (ByteArray) -> Value): Flow<KafkaMessage<Key, Partition, Value, Output>> {
    return transform { kafkaMessage ->
        @Suppress("UNCHECKED_CAST")
        when (kafkaMessage) {
            is Record -> emit(
                Record(
                    kafkaMessage.consumerRecord,
                    kafkaMessage.key,
                    kafkaMessage.partitionKey,
                    block.invoke(kafkaMessage.consumerRecord.value()),
                    kafkaMessage.output
                )
            )
            else -> emit(kafkaMessage as KafkaMessage<Key, Partition, Value, Output>)
        }
    }
}

public fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output>>.values(): Flow<Value> {
    return filterIsInstance<Record<Key, Partition, Value, Output>>()
        .map { it.value }
}

public fun <KeyIn, KeyOut, PartitionIn, PartitionOut, ValueIn, ValueOut, OutputIn, OutputOut> Flow<KafkaMessage<KeyIn, PartitionIn, ValueIn, OutputIn>>.mapRecord(
    block: suspend (Record<KeyIn, PartitionIn, ValueIn, OutputIn>) -> Record<KeyOut, PartitionOut, ValueOut, OutputOut>
): Flow<KafkaMessage<KeyOut, PartitionOut, ValueOut, OutputOut>> {
    return map { message ->
        if (message is Record) {
            block.invoke(message)
        } else {
            @Suppress("UNCHECKED_CAST")
            message as KafkaMessage<KeyOut, PartitionOut, ValueOut, OutputOut>
        }
    }
}

public fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output>>.onEachRecord(block: suspend (Record<Key, Partition, Value, Output>) -> Unit): Flow<KafkaMessage<Key, Partition, Value, Output>> {
    return onEach { message ->
        if (message is Record)
            block.invoke(message)
    }
}

public fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output>>.onStartConsuming(block: suspend (StartConsuming<Key, Partition, Value, Output>) -> Unit): Flow<KafkaMessage<Key, Partition, Value, Output>> {
    return onEach { message ->
        if (message is StartConsuming)
            block.invoke(message)
    }
}

public fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output>>.onStopConsuming(block: suspend (StopConsuming<Key, Partition, Value, Output>) -> Unit): Flow<KafkaMessage<Key, Partition, Value, Output>> {
    return onEach { message ->
        if (message is StopConsuming)
            block.invoke(message)
    }
}

public fun <Key, Partition, Value, Output> Flow<KafkaMessage<Unit, Unit, Unit, Output>>.deserializeUsing(topicDescriptor: TopicDescriptor<Key, Partition, Value>, onDeserializationException: suspend (Throwable) -> Unit = { it.printStackTrace() })
        : Flow<KafkaMessage<Key, Partition, Value?, Output>> {
    return transform { message ->
        if (message is Record) {
            try {
                val key = topicDescriptor.deserializeKey(message.consumerRecord.key())
                val partitionKey = topicDescriptor.partitionKey(key)
                val value = topicDescriptor.deserializeValue(message.consumerRecord.value())
                emit(Record(message.consumerRecord, key, partitionKey, value, message.output))
            } catch (throwable: Throwable) {
                runCatching {
                    onDeserializationException.invoke(throwable)
                }.onFailure {
                    it.printStackTrace()
                    throwable.printStackTrace()
                }
            }
        } else {
            @Suppress("UNCHECKED_CAST")
            emit(message as KafkaMessage<Key, Partition, Value?, Output>)
        }
    }
}