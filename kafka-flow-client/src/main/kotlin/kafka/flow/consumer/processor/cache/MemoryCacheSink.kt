package kafka.flow.consumer.processor.cache

import be.delta.flow.time.seconds
import kafka.flow.consumer.KafkaFlowConsumer
import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.processor.Sink
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Duration
import java.time.Instant

@Suppress("UNCHECKED_CAST")
public class MemoryCacheSink<Key, PartitionKey, Value>(
    private val retention: Duration?,
    private val cleanupInterval: Duration,
) : Sink<Key, PartitionKey, Value?, Unit, WithoutTransaction>, Cache<Key, Value> {
    private var client: KafkaFlowConsumer<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>? = null
    private val data: HashMap<Key, TimedValue<Value>> = HashMap()
    private val mutex = Mutex()

    override suspend fun record(
        consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
        key: Key,
        partitionKey: PartitionKey,
        value: Value?,
        timestamp: Instant,
        output: Unit,
        transaction: WithoutTransaction
    ) {
        if (timestamp.isWithinRetention()) {
            mutex.withLock {
                if (value != null) {
                    data[key] = TimedValue(timestamp, value)
                } else {
                    data.remove(key)
                }
            }
        }
    }

    public override suspend fun get(key: Key): Value? {
        waitClient()
        return mutex.withLock { data[key]?.value }
    }

    public override suspend fun keys(): List<Key> = mutex.withLock {
        waitClient()
        data.keys.toList()
    }

    public override suspend fun values(): List<Value> = mutex.withLock {
        waitClient()
        data.values.map { it.value }
    }

    public override suspend fun all(): Map<Key, Value> = mutex.withLock {
        waitClient()
        data.mapValues { it.value.value }.toMap()
    }

    private suspend fun waitClient() {
        if (client == null) {
            val timeout = Instant.now() + 10.seconds()
            while (client == null && Instant.now() < timeout) {
                delay(10)
            }
            checkNotNull(client) { "Consumer isn't stated, please start the client before using the cache" }
        }
        client!!.waitUntilUpToDate()
    }

    override suspend fun startConsuming(client: KafkaFlowConsumer<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>) {
        this.client = client
        if (retention == null) return
        CoroutineScope(currentCoroutineContext()).launch {
            while (true) {
                delay(cleanupInterval.toMillis())
                mutex.withLock {
                    val keysToRemove = data.filterValues { !it.timestamp.isWithinRetention() }.keys
                    keysToRemove.forEach { data.remove(it) }
                }
            }
        }
    }

    private fun Instant.isWithinRetention(): Boolean = if (retention == null) true else this > Instant.now() - retention

    private data class TimedValue<Value>(val timestamp: Instant, val value: Value)
}
