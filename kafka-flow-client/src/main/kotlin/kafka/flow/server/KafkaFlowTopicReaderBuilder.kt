package kafka.flow.server

import kafka.flow.TopicDescriptor
import kafka.flow.server.api.ConsumerBuilderConfig
import kafka.flow.server.api.ConsumerBuilderStep1GroupId
import java.util.*

public class KafkaFlowTopicReaderBuilder<Key, PartitionKey, Value>(private val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>, private val serverProperties: Properties) {
    public fun consumer(): ConsumerBuilderStep1GroupId<Key, PartitionKey, Value> = ConsumerBuilderStep1GroupId(ConsumerBuilderConfig(topicDescriptor, serverProperties))
}


