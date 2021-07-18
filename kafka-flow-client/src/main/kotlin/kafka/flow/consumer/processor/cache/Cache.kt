package kafka.flow.consumer.processor.cache

public interface Cache<Key, Value> {
    public suspend fun get(key: Key): Value?
    public suspend fun keys(): List<Key>
    public suspend fun values(): List<Value>
    public suspend fun all(): Map<Key, Value>
}