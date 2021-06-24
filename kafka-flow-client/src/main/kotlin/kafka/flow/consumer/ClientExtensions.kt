package kafka.flow.consumer

import kotlinx.coroutines.flow.*

public suspend fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output>>.collectValues(block: (Value) -> Unit) {
    this.filterIsInstance<Record<Key, Partition, Value, Output>>()
        .collect { block.invoke(it.value) }
}

public suspend fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Unit, Output>>.deserializeValue(block: (ByteArray) -> Value): Flow<KafkaMessage<Key, Partition, Value, Output>> {
    return transform { kafkaMessage ->
        @Suppress("UNCHECKED_CAST")
        when (kafkaMessage) {
            is Record -> emit(
                Record(
                    kafkaMessage.original,
                    kafkaMessage.key,
                    kafkaMessage.partitionKey,
                    block.invoke(kafkaMessage.original.value()),
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