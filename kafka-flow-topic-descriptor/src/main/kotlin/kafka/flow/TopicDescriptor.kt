package kafka.flow

import com.sangupta.murmur.Murmur2
import java.time.Instant
import kotlin.math.absoluteValue

public interface TopicDescriptor<Key, PartitionKey, Value> {
    public val name: String
    public val partitionNumber: Int
    public val config: Map<String, String>

    public fun serializeKey(key: Key): ByteArray
    public fun deserializeKey(key: ByteArray): Key

    public fun serializePartitionKey(partitionKey: PartitionKey): ByteArray

    public fun serializeValue(value: Value?): ByteArray?
    public fun deserializeValue(value: ByteArray?): Value?

    public fun key(value: Value): Key
    public fun partitionKey(key: Key): PartitionKey
    public fun timestamp(value: Value): Instant

    public fun partition(partitionKey: PartitionKey): Int = (hash(serializePartitionKey(partitionKey)).absoluteValue % partitionNumber).toInt()

    private fun hash(data: ByteArray): Long = Murmur2.hash(data, data.size, 0xe17a1465)
}