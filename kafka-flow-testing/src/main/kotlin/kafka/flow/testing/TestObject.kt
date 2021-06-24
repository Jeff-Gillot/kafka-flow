package kafka.flow.testing

import org.testcontainers.utility.Base58
import java.time.Instant

public data class TestObject(
    val key: Key,
    val timestamp: Instant,
    val value: String,
) {
    init {
        require(!value.contains('/')) { "The value cannot contains '/'" }
    }

    public data class Key(
        val id: String,
        val partitionKey: String
    ) {
        init {
            require(!id.contains('/')) { "The id cannot contains '/'" }
            require(!partitionKey.contains('/')) { "The partitionKey cannot contains '/'" }
        }
    }

    public companion object {
        public fun random(): TestObject = TestObject(Key(Base58.randomString(10), Base58.randomString(5)), Instant.now(), Base58.randomString(20))
    }
}
