package kafka.flow.producer

import kafka.flow.TopicDescriptor
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
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

    public fun close() {
        delegate.close()
    }
}
