package kafka.flow.server

import be.delta.flow.time.minute
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.processor.cache.CacheConsumer
import kafka.flow.server.api.ConsumerBuilderConfig
import kafka.flow.server.api.ConsumerBuilderStep1GroupId
import java.time.Duration
import java.util.*

public class KafkaFlowTopicReaderBuilder<Key, PartitionKey, Value>(
    private val topicDescriptors: List<TopicDescriptor<Key, PartitionKey, Value>>,
    private val serverProperties: Properties
) {
    public fun consumer(): ConsumerBuilderStep1GroupId<Key, PartitionKey, Value> = ConsumerBuilderStep1GroupId(ConsumerBuilderConfig(topicDescriptors, serverProperties))

    public fun cacheConsumer(retention: Duration? = null, cleanupInterval: Duration = 1.minute()): CacheConsumer<Key, PartitionKey, Value> {
        return CacheConsumer(serverProperties, topicDescriptors, retention, cleanupInterval)
    }
}


