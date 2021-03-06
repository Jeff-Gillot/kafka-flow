package kafka.flow.producer

import invokeAndThrow
import java.time.Instant
import java.util.Properties
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.utils.logger
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArraySerializer

public class KafkaFlowTopicProducer<Key, PartitionKey, Value>(private val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>, private val config: Properties) {
    private val logger = logger()

    private val delegate: KafkaProducer<ByteArray, ByteArray> by lazy {
        KafkaProducer(config, ByteArraySerializer(), ByteArraySerializer())
    }

    public suspend fun send(value: Value, transaction: MaybeTransaction) {
        send(value) {
            it.onFailure { throwable ->
                logger.warn("Error while sending record to kafka, rolling back the transaction $transaction for $value, ${throwable.message}")
                transaction.rollback()
            }
            it.onSuccess { transaction.unlock() }
        }
    }

    public suspend fun send(value: Value, callback: suspend (Result<RecordMetadata>) -> Unit = {}) {
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
        ) { metadata, exception ->
            runBlocking {
                if (exception != null) callback.invokeAndThrow(Result.failure(exception))
                else callback.invokeAndThrow(Result.success(metadata))
            }
        }
    }

    public suspend fun sendTombstone(key: Key, timestamp: Instant, transaction: MaybeTransaction) {
        sendTombstone(key, timestamp) {
            it.onFailure { throwable ->
                logger.warn("Error while sending tombstone to kafka, rolling back the transaction $transaction for $key, ${throwable.message}")
                transaction.rollback()
            }
            it.onSuccess { transaction.unlock() }
        }
    }

    public suspend fun sendTombstone(key: Key, timestamp: Instant, callback: suspend (Result<RecordMetadata>) -> Unit = {}) {
        val partitionKey = topicDescriptor.partitionKey(key)
        delegate.send(
            ProducerRecord(
                topicDescriptor.name,
                topicDescriptor.partition(partitionKey),
                timestamp.toEpochMilli(),
                topicDescriptor.serializeKey(key),
                null as ByteArray?
            )
        ) { metadata, exception ->
            runBlocking {
                if (exception != null) callback.invokeAndThrow(Result.failure(exception))
                else callback.invokeAndThrow(Result.success(metadata))
            }
        }
    }

    public fun close() {
        delegate.close()
    }
}
