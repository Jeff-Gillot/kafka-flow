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
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

public class GroupingProcessor<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>(
    private val processorTimeout: Duration,
    private val channelCapacity: Int,
    private val flowFactory: suspend (Flow<KafkaMessage<Key, PartitionKey, Value, Output, Transaction>>, partitionKey: PartitionKey) -> Unit
) : Sink<Key, PartitionKey, Value, Output, Transaction> {
    private lateinit var processorTimeoutLoop: Job
    private lateinit var client: KafkaFlowConsumer<Flow<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>>
    private val processors = ConcurrentHashMap<PartitionKey, Channel<KafkaMessage<Key, PartitionKey, Value, Output, Transaction>>>()
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
                processorsToRemove.forEach {
                    processorLastMessage.remove(it!!)
                    processorsPartitions.remove(it)
                    val channel = processors.remove(it)
                    channel?.send(StopConsuming())
                    channel?.close()
                }
            }
        }
    }

    override suspend fun stopConsuming() {
        processors.values.toList().forEach { processor ->
            CoroutineScope(currentCoroutineContext()).launch {
                processor.send(StopConsuming())
            }
        }
    }

    override suspend fun completion() {
        processorTimeoutLoop.cancel()
        CoroutineScope(currentCoroutineContext()).launch {
            delay(50)
            processors.values.forEach { it.close() }
            processors.clear()
        }
    }

    override suspend fun partitionRevoked(revokedPartition: List<TopicPartition>, assignment: List<TopicPartition>) {
        val processorsToRevoke = processorsPartitions
            .filterValues { revokedPartition.contains(it) }
            .keys
            .associateWith { processors[it] }
            .toMap()
        processorsToRevoke.values.toList().forEach { processor ->
            if (processor != null) {
                CoroutineScope(currentCoroutineContext()).launch {
                    processor.send(StopConsuming())
                    processor.close()
                }
            }
        }
        processorsToRevoke.keys.toList().forEach { processors.remove(it!!) }
        processorsToRevoke.keys.toList().forEach { processorsPartitions.remove(it!!) }
        processorsToRevoke.keys.toList().forEach { processorLastMessage.remove(it!!) }
    }

    private suspend fun getOrCreateProcessor(partitionKey: PartitionKey, consumerRecord: ConsumerRecord<ByteArray, ByteArray>): Channel<KafkaMessage<Key, PartitionKey, Value, Output, Transaction>> {
        processorLastMessage[partitionKey] = Instant.now()
        var channel = processors[partitionKey]
        if (channel != null) return channel
        channel = Channel(channelCapacity)
        processors[partitionKey] = channel
        processorsPartitions[partitionKey] = TopicPartition(consumerRecord.topic(), consumerRecord.partition())
        val flow = channel.consumeAsFlow()
        CoroutineScope(currentCoroutineContext()).launch {
            flowFactory.invoke(flow, partitionKey)
        }
        channel.send(StartConsuming(client))
        return channel
    }
}