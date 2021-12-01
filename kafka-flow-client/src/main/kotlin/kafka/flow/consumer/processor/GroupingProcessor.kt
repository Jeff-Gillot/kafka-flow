package kafka.flow.consumer.processor

import be.delta.flow.time.seconds
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kafka.flow.consumer.KafkaFlowConsumer
import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.Record
import kafka.flow.consumer.StartConsuming
import kafka.flow.consumer.StopConsuming
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

@Suppress("DeferredResultUnused")
public class GroupingProcessor<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>(
    private val processorTimeout: Duration,
    private val channelCapacity: Int,
    private val flowFactory: suspend (Flow<KafkaMessage<Key, PartitionKey, Value, Output, Transaction>>, partitionKey: PartitionKey) -> Unit
) : Sink<Key, PartitionKey, Value, Output, Transaction> {
    private lateinit var processorTimeoutLoop: Job
    private lateinit var client: KafkaFlowConsumer<Flow<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>>
    private val processors = ConcurrentHashMap<PartitionKey, Deferred<Channel<KafkaMessage<Key, PartitionKey, Value, Output, Transaction>>>>()
    private val processorsPartitions = ConcurrentHashMap<PartitionKey, TopicPartition>()
    private val processorLastMessage = ConcurrentHashMap<PartitionKey, Instant>()

    override suspend fun record(
        consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
        key: Key,
        partitionKey: PartitionKey,
        value: Value,
        timestamp: Instant,
        output: Output,
        transaction: Transaction
    ) {
        val channel = getOrCreateProcessor(partitionKey, consumerRecord)
        channel.send(Record(consumerRecord, key, partitionKey, value, timestamp, output, transaction))
    }

    override suspend fun startConsuming(client: KafkaFlowConsumer<Flow<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>>) {
        this.client = client

        processorTimeoutLoop = CoroutineScope(currentCoroutineContext()).launch {
            while (true) {
                delay(10.seconds().toMillis())
                val processorsToRemove = processorLastMessage
                    .filterValues { it < Instant.now() - processorTimeout }
                    .keys
                    .toList()

                processorsToRemove.forEach { partitionKey ->
                    closeAndRemoveProcessor(partitionKey)
                }
            }
        }
    }

    override suspend fun stopConsuming() {
        processorTimeoutLoop.cancel()
        processors.keys.toList().forEach { partitionKey ->
            closeAndRemoveProcessor(partitionKey)
        }
    }

    override suspend fun completion() {
        stopConsuming()
    }

    override suspend fun partitionRevoked(revokedPartition: List<TopicPartition>, assignment: List<TopicPartition>) {
        val partitionKeysToRevoke = processorsPartitions
            .filterValues { revokedPartition.contains(it) }
            .keys

        partitionKeysToRevoke.forEach { partitionKey: PartitionKey ->
            closeAndRemoveProcessor(partitionKey)
        }
    }

    private suspend fun closeAndRemoveProcessor(partitionKey: PartitionKey) {
        val coroutineContext = currentCoroutineContext()
        processors.compute(partitionKey) { key: PartitionKey, processor ->
            processorsPartitions.remove(key!!)
            processorLastMessage.remove(key)
            CoroutineScope(coroutineContext).launch {
                processor?.await()?.send(StopConsuming())
                processor?.await()?.close()
            }
            null
        }
    }

    private suspend fun getOrCreateProcessor(
        partitionKey: PartitionKey,
        consumerRecord: ConsumerRecord<ByteArray, ByteArray>
    ): Channel<KafkaMessage<Key, PartitionKey, Value, Output, Transaction>> {
        val coroutineContext = currentCoroutineContext()
        return processors.compute(partitionKey) { _, value ->
            println("$partitionKey $value")
            processorLastMessage[partitionKey] = Instant.now()
            if (value != null) {
                value
            } else {
                processorsPartitions[partitionKey] = TopicPartition(consumerRecord.topic(), consumerRecord.partition())
                CoroutineScope(coroutineContext).async {
                    val channel = Channel<KafkaMessage<Key, PartitionKey, Value, Output, Transaction>>(channelCapacity)
                    val flow = channel.consumeAsFlow()
                    CoroutineScope(coroutineContext).launch {
                        flowFactory.invoke(flow, partitionKey)
                    }
                    channel.send(StartConsuming(client))
                    channel
                }
            }
        }!!.await()
    }
}