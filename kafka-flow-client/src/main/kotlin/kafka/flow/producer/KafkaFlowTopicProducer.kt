package kafka.flow.producer

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.with.group.id.Transaction
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.time.Instant
import java.util.*

public class KafkaFlowTopicProducer<Key, PartitionKey, Value>(private val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>, private val config: Properties) {

    private val delegate: KafkaProducer<ByteArray, ByteArray> by lazy {
        KafkaProducer(config, ByteArraySerializer(), ByteArraySerializer())
    }

    public fun send(value: Value) {
        val key = topicDescriptor.key(value)
        val partitionKey = topicDescriptor.partitionKey(key)
        delegate.send(
            ProducerRecord(
                topicDescriptor.name,
                topicDescriptor.partition(partitionKey),
                topicDescriptor.timestamp(value).toEpochMilli(),
                topicDescriptor.serializeKey(key),
                topicDescriptor.serializeValue(value)
            )
        ) { _, exception -> exception?.printStackTrace() }
    }

    public suspend fun send(value: Value, transaction: Transaction) {
        val key = topicDescriptor.key(value)
        val partitionKey = topicDescriptor.partitionKey(key)
        delegate.send(
            ProducerRecord(
                topicDescriptor.name,
                topicDescriptor.partition(partitionKey),
                topicDescriptor.timestamp(value).toEpochMilli(),
                topicDescriptor.serializeKey(key),
                topicDescriptor.serializeValue(value)
            )
        ) { _, exception ->
            runBlocking {
                if (exception != null) transaction.rollback()
                else transaction.unlock()
            }
        }
    }

    public suspend fun sendTombstone(key: Key, timestamp: Instant, transaction: Transaction) {
        val partitionKey = topicDescriptor.partitionKey(key)
        delegate.send(
            ProducerRecord(
                topicDescriptor.name,
                topicDescriptor.partition(partitionKey),
                timestamp.toEpochMilli(),
                topicDescriptor.serializeKey(key),
                null as ByteArray?
            )
        ) { _, exception ->
            runBlocking {
                if (exception != null) transaction.rollback()
                else transaction.unlock()
            }
        }
    }

    public fun close() {
        delegate.close()
    }
}
