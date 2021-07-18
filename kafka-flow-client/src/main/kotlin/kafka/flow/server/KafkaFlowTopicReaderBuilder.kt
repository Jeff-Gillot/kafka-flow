package kafka.flow.server

import be.delta.flow.time.minute
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.StartOffsetPolicy
import kafka.flow.consumer.collect
import kafka.flow.consumer.processor.cache.CacheConsumer
import kafka.flow.consumer.processor.cache.MemoryCacheSink
import kafka.flow.server.api.ConsumerBuilderConfig
import kafka.flow.server.api.ConsumerBuilderStep1GroupId
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import java.time.Duration
import java.util.*

public class KafkaFlowTopicReaderBuilder<Key, PartitionKey, Value>(private val topicDescriptors: List<TopicDescriptor<Key, PartitionKey, Value>>, private val serverProperties: Properties) {
    public fun consumer(): ConsumerBuilderStep1GroupId<Key, PartitionKey, Value> = ConsumerBuilderStep1GroupId(ConsumerBuilderConfig(topicDescriptors, serverProperties))

    public suspend fun startCacheConsumer(retention: Duration? = null, interval: Duration = 1.minute()): CacheConsumer<Key, Value> {
        val sink = MemoryCacheSink<Key, PartitionKey, Value>(retention, interval)
        val consumer = consumer()
            .withoutGroupId()
            .startFromPolicy(if (retention != null) StartOffsetPolicy.specificOffsetFromNow(retention) else StartOffsetPolicy.earliest())
            .consumeUntilStopped()
            .readAllPartitions()

        CoroutineScope(currentCoroutineContext()).launch {
            consumer
                .startConsuming()
                .collect(sink)
        }

        return CacheConsumer(consumer, sink)
    }
}


