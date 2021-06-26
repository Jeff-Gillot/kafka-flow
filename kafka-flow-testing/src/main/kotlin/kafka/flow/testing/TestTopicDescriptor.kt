package kafka.flow.testing

import kafka.flow.TopicDescriptor
import java.time.Instant

public class TestTopicDescriptor(override val name: String) : TopicDescriptor<TestObject.Key, String, TestObject> {
    override val partitionNumber: Int
        get() = 12
    override val config: Map<String, String>
        get() = emptyMap()

    override fun serializeKey(key: TestObject.Key): ByteArray = "${key.partitionKey}/${key.id}".toByteArray()
    override fun deserializeKey(key: ByteArray): TestObject.Key {
        val parts = String(key).split('/')
        return TestObject.Key(parts[0], parts[1])
    }

    override fun serializePartitionKey(partitionKey: String): ByteArray = partitionKey.toByteArray()

    override fun serializeValue(value: TestObject?): ByteArray? = value?.let { "${value.key.partitionKey}/${value.key.id}/${value.timestamp}/${value.value}".toByteArray() }
    override fun deserializeValue(value: ByteArray?): TestObject? {
        if (value == null) return null
        val parts = String(value).split('/')
        return TestObject(TestObject.Key(parts[1], parts[0]), Instant.parse(parts[2]), parts[3])
    }

    override fun key(value: TestObject): TestObject.Key = value.key
    override fun partitionKey(key: TestObject.Key): String = key.partitionKey

    override fun timestamp(value: TestObject): Instant = value.timestamp

    public companion object {
        private var count = 0
        public fun next(): TestTopicDescriptor {
            return TestTopicDescriptor("test-Topic-${count++.toString().padStart(3, '0')}")
        }
    }
}