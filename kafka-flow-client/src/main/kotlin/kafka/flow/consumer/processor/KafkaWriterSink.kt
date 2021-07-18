package kafka.flow.consumer.processor

import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.producer.KafkaOutput
import kafka.flow.producer.TopicDescriptorRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Instant

@Suppress("UNCHECKED_CAST")
public class KafkaWriterSink<Key, PartitionKey, Value, Transaction : MaybeTransaction> : Sink<Key, PartitionKey, Value, KafkaOutput, Transaction> {
    override suspend fun record(
        consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
        key: Key,
        partitionKey: PartitionKey,
        value: Value,
        timestamp: Instant,
        output: KafkaOutput,
        transaction: Transaction
    ) {
        output.records.forEach { outputRecord ->
            val record: TopicDescriptorRecord<Any, Any, Any> = outputRecord as TopicDescriptorRecord<Any, Any, Any>
            transaction.lock()
            when (record) {
                is TopicDescriptorRecord.Record -> outputRecord.kafkaServer.on(record.topicDescriptor).send(record.value, transaction)
                is TopicDescriptorRecord.Tombstone -> outputRecord.kafkaServer.on(record.topicDescriptor).sendTombstone(record.key, record.timestamp, transaction)
            }
        }
        transaction.unlock()
    }
}