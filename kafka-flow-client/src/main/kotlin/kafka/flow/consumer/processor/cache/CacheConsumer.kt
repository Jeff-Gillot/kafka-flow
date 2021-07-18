package kafka.flow.consumer.processor.cache

import kafka.flow.consumer.KafkaClient

public class CacheConsumer<Key, Value>(private val client: KafkaClient, private val cache: Cache<Key, Value>) : KafkaClient by client, Cache<Key, Value> by cache