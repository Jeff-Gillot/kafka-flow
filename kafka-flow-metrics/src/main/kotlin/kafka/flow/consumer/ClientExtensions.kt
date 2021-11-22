@file:Suppress("UNCHECKED_CAST")

package kafka.flow.consumer

import kafka.flow.KafkaMetricLogger
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.producer.KafkaOutput
import kotlin.system.measureNanoTime
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<List<KafkaMessage<Key, Partition, Value, Unit, Transaction>>>.mapValuesToOutput(
    kafkaMetricLogger: KafkaMetricLogger,
    block: suspend (List<Pair<Key, Value>>) -> Output
): Flow<Pair<List<KafkaMessage<Key, Partition, Value, Unit, Transaction>>, Output>> {
    return map { messages ->
        val records = messages.filterIsInstance<Record<Key, Partition, Value, Unit, Transaction>>()
        val keyValues = records.map { it.key to it.value }

        var result: Output
        val time = measureNanoTime {
            result = block.invoke(keyValues)
        }

        kafkaMetricLogger.registerInput(records, time)

        val output = result
        if (output is KafkaOutput) {
            kafkaMetricLogger.registerOutput(output)
        }

        messages to output
    }
}

@Suppress("UNCHECKED_CAST")
public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Unit, Transaction>>.mapValueToOutput(
    kafkaMetricLogger: KafkaMetricLogger,
    block: suspend (Key, Value) -> Output
): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return map { record ->
        if (record is Record) {
            var result: Output
            val time = measureNanoTime {
                result = block.invoke(record.key, record.value)
            }

            kafkaMetricLogger.registerInput(record, time)

            val output = result
            if (output is KafkaOutput) {
                kafkaMetricLogger.registerOutput(output)
            }

            Record(record.consumerRecord, record.key, record.partitionKey, record.value, record.timestamp, output, record.transaction)
        } else {
            record as KafkaMessage<Key, Partition, Value, Output, Transaction>
        }
    }
}