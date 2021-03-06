package kafka.flow.consumer.processor.cache

import kafka.flow.TopicDescriptor
import kafka.flow.consumer.KafkaFlowConsumer
import kafka.flow.consumer.StartOffsetPolicy
import kafka.flow.consumer.collect
import kafka.flow.server.KafkaFlowTopicReaderBuilder
import java.time.Duration
import java.util.*
import org.apache.kafka.common.TopicPartition

public class CacheConsumer<Key, PartitionKey, Value>(
    properties: Properties,
    topicDescriptors: List<TopicDescriptor<Key, PartitionKey, Value>>,
    retention: Duration?,
    cleanupInterval: Duration
) : KafkaFlowConsumer<Unit>, Cache<Key, Value> {
    private val cache = MemoryCacheSink<Key, PartitionKey, Value>(retention, cleanupInterval)
    private val delegate = KafkaFlowTopicReaderBuilder(topicDescriptors, properties)
        .consumer()
        .withoutGroupId()
        .startFromPolicy(if (retention != null) StartOffsetPolicy.specificOffsetFromNow(retention) else StartOffsetPolicy.earliest())
        .consumeUntilStopped()
        .readAllPartitions()

    override suspend fun startConsuming(onDeserializationException: suspend (Throwable) -> Unit) {
        delegate
            .startConsuming()
            .collect(cache)
    }

    override fun stop(): Unit = delegate.stop()
    override fun isRunning(): Boolean = delegate.isRunning()
    override fun isUpToDate(): Boolean = delegate.isRunning()
    override fun lag(): Long? = delegate.lag()
    override fun lags(): Map<TopicPartition, Long?>? = delegate.lags()
    override val assignment: List<TopicPartition> get() = delegate.assignment

    override suspend fun get(key: Key): Value? = cache.get(key)
    override suspend fun keys(): List<Key> = cache.keys()
    override suspend fun values(): List<Value> = cache.values()
    override suspend fun all(): Map<Key, Value> = cache.all()
}