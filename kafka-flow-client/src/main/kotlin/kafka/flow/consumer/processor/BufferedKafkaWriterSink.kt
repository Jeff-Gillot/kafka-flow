package kafka.flow.consumer.processor

import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.Record
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.producer.KafkaOutput
import kafka.flow.producer.TopicDescriptorRecord
import kotlinx.coroutines.flow.FlowCollector

@Suppress("UNCHECKED_CAST")
public class BufferedKafkaWriterSink<Key, PartitionKey, Value, Transaction : MaybeTransaction> {

    public suspend fun handleRecords(value: Pair<List<KafkaMessage<Key, PartitionKey, Value, Unit, Transaction>>, KafkaOutput>) {
        val records = value.first.filterIsInstance<Record<Key, PartitionKey, Value, Unit, Transaction>>()
        val outputs = value.second

        records.forEach { record ->
            repeat(outputs.records.size) { record.transaction.lock() }
        }

        outputs.records.forEach { outputRecord ->
            val output: TopicDescriptorRecord<Any, Any, Any> = outputRecord as TopicDescriptorRecord<Any, Any, Any>
            when (output) {
                is TopicDescriptorRecord.Record -> output.kafkaServer.on(output.topicDescriptor).send(output.value) { result ->
                    result.onSuccess { records.forEach { it.transaction.unlock() } }
                    result.onFailure { records.forEach { it.transaction.rollback() } }
                }
                is TopicDescriptorRecord.Tombstone -> output.kafkaServer.on(output.topicDescriptor).sendTombstone(output.key, output.timestamp) { result ->
                    result.onSuccess { records.forEach { it.transaction.unlock() } }
                    result.onFailure { records.forEach { it.transaction.rollback() } }
                }
            }
        }
        records.forEach { it.transaction.unlock() }
    }
}