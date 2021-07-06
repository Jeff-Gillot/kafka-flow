package kafka.flow.consumer.processor

import be.delta.flow.time.seconds
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.*
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.time.Instant

public class GroupingProcessor<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>(
    private val topicDescriptor: TopicDescriptor<Key, PartitionKey, Value>,
    private val processorTimeout: Duration,
    private val channelCapacity: Int,
    private val flowFactory: suspend (Flow<KafkaMessage<Key, PartitionKey, Value, Output, Transaction>>, partitionKey: PartitionKey) -> Unit
) : Sink<Key, PartitionKey, Value, Output, Transaction> {
    private lateinit var processorTimeoutLoop: Job
    private lateinit var client: KafkaFlowConsumer<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>
    private val processors = mutableMapOf<PartitionKey, Channel<KafkaMessage<Key, PartitionKey, Value, Output, Transaction>>>()
    private val processorLastMessage = mutableMapOf<PartitionKey, Instant>()
    private val mutex = Mutex()

    override suspend fun record(
        consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
        key: Key,
        partitionKey: PartitionKey,
        value: Value,
        output: Output,
        transaction: Transaction
    ) {
        val channel = getOrCreateProcessor(partitionKey)
        channel.send(Record(consumerRecord, key, partitionKey, value, output, transaction))
    }

    override suspend fun startConsuming(client: KafkaFlowConsumer<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>) {
        this.client = client

        processorTimeoutLoop = CoroutineScope(currentCoroutineContext()).launch {
            while (true) {
                delay(1.seconds().toMillis())
                processorLastMessage
                    .filterValues { it < Instant.now() - processorTimeout }
                    .keys
                    .toList()
                    .forEach {
                        processorLastMessage.remove(it)
                        val channel = mutex.withLock { processors.remove(it) }
                        channel?.send(StopConsuming())
                        channel?.close()
                    }
            }
        }
    }

    override suspend fun stopConsuming() {
        mutex.withLock {
            processors.values.forEach { processor ->
                CoroutineScope(currentCoroutineContext()).launch {
                    processor.send(StopConsuming())
                }
            }
        }
    }

    override suspend fun completion() {
        processorTimeoutLoop.cancel()
        CoroutineScope(currentCoroutineContext()).launch {
            delay(50)
            mutex.withLock {
                processors.values.forEach { it.close() }
                processors.clear()
            }
        }
    }

    override suspend fun partitionRevoked(revokedPartition: List<TopicPartition>, assignment: List<TopicPartition>) {
        val revokedPartitionNumbers = revokedPartition.map { it.partition() }
        mutex.withLock {
            val processorsToRevoke = processors.filterKeys { revokedPartitionNumbers.contains(topicDescriptor.partition(it)) }.toMap()
            processorsToRevoke.values.forEach { processor ->
                CoroutineScope(currentCoroutineContext()).launch {
                    processor.send(StopConsuming())
                    processor.close()
                }
            }
            processorsToRevoke.keys.forEach { processors.remove(it) }
        }
    }

    private suspend fun getOrCreateProcessor(partitionKey: PartitionKey): Channel<KafkaMessage<Key, PartitionKey, Value, Output, Transaction>> {
        return mutex.withLock {
            processorLastMessage[partitionKey] = Instant.now()
            var channel = processors[partitionKey]
            if (channel != null) return channel
            channel = Channel(channelCapacity)
            processors[partitionKey] = channel
            val flow = channel.consumeAsFlow()
            CoroutineScope(currentCoroutineContext()).launch {
                flowFactory.invoke(flow, partitionKey)
            }
            channel.send(StartConsuming(client))
            channel
        }
    }
}