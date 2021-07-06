package kafka.flow.consumer.processor

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.Record
import kafka.flow.consumer.with.group.id.MaybeTransaction
import org.apache.kafka.clients.consumer.ConsumerRecord

public class TopicDescriptorDeserializerProcessor<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>(
    private val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>,
    private val onDeserializationException: suspend (Throwable) -> Unit
) : TransformProcessor<Unit, Unit, Unit, Output, Transaction, Key, PartitionKey, Value?, Output, Transaction> {
    override suspend fun record(
        consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
        key: Unit,
        partitionKey: Unit,
        value: Unit,
        output: Output,
        transaction: Transaction
    ): Record<Key, PartitionKey, Value?, Output, Transaction>? {
        try {
            val newKey = topicDescriptor.deserializeKey(consumerRecord.key())
            val newPartitionKey = topicDescriptor.partitionKey(newKey)
            val newValue = topicDescriptor.deserializeValue(consumerRecord.value())
            return Record(consumerRecord, newKey, newPartitionKey, newValue, output, transaction)
        } catch (throwable: Throwable) {
            runCatching {
                onDeserializationException.invoke(throwable)
            }.onFailure {
                it.printStackTrace()
                throwable.printStackTrace()
            }
            return null
        }
    }

}