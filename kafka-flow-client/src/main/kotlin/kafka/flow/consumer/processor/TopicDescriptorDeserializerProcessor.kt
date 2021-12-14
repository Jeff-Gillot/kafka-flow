package kafka.flow.consumer.processor

import invokeAndThrow
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.Record
import kafka.flow.consumer.with.group.id.MaybeTransaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Instant

public class TopicDescriptorDeserializerProcessor<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>(
    topicDescriptors: List<TopicDescriptor<Key, PartitionKey, Value>>,
    private val onDeserializationException: suspend (Throwable) -> Unit
) : TransformProcessor<Unit, Unit, Unit, Output, Transaction, Key, PartitionKey, Value?, Output, Transaction> {

    private val topicDescriptorsByName = topicDescriptors.associateBy { it.name }

    override suspend fun record(
        consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
        key: Unit,
        partitionKey: Unit,
        value: Unit,
        timestamp: Instant,
        output: Output,
        transaction: Transaction
    ): Record<Key, PartitionKey, Value?, Output, Transaction>? {
        try {
            val topicDescriptor = topicDescriptorsByName[consumerRecord.topic()]!!
            val newKey = topicDescriptor.deserializeKey(consumerRecord.key())
            val newPartitionKey = topicDescriptor.partitionKey(newKey)
            val newValue = topicDescriptor.deserializeValue(consumerRecord.value())
            val newTimestamp = newValue?.let { topicDescriptor.timestamp(it) }?:timestamp
            return Record(consumerRecord, newKey, newPartitionKey, newValue, newTimestamp, output, transaction)
        } catch (throwable: Throwable) {
            runCatching {
                onDeserializationException.invokeAndThrow(throwable)
            }.onFailure {
                it.printStackTrace()
                throwable.printStackTrace()
            }
            return null
        }
    }

}