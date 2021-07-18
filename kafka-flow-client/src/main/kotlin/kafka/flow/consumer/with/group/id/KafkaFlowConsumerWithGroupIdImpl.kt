package kafka.flow.consumer.with.group.id

import be.delta.flow.time.milliseconds
import be.delta.flow.time.seconds
import kafka.flow.consumer.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.*
import kotlin.coroutines.EmptyCoroutineContext

public class KafkaFlowConsumerWithGroupIdImpl(
    clientProperties: Properties,
    private val topics: List<String>,
    private val startOffsetPolicy: StartOffsetPolicy,
    private val autoStopPolicy: AutoStopPolicy
) : KafkaFlowConsumerWithGroupId<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>> {
    private val properties = properties(clientProperties)
    private val logger = LoggerFactory.getLogger(TransactionManager::class.java)
    private val delegate: KafkaConsumer<ByteArray, ByteArray> = KafkaConsumer(properties, ByteArrayDeserializer(), ByteArrayDeserializer())
    private var running = false
    private var stopRequested: Boolean = false
    private var startInstant: Instant? = null
    private var endOffsets: Map<TopicPartition, Long> = emptyMap()
    private var assignment: List<TopicPartition> = emptyList()
    private val partitionChangedMessages = mutableListOf<PartitionChangedMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>()
    private val delegateMutex = Mutex()
    private val pollDuration = 10.milliseconds()

    init {
        requireNotNull(clientProperties[ConsumerConfig.GROUP_ID_CONFIG]) { "${ConsumerConfig.GROUP_ID_CONFIG} must be set" }
    }

    override suspend fun startConsuming(onDeserializationException: suspend (Throwable) -> Unit): Flow<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>> {
        subscribe()
        return createConsumerChannel()
            .consumeAsFlow()
            .onCompletion { cleanup() }
    }

    override fun isRunning(): Boolean = running

    override suspend fun isUpToDate(): Boolean {
        if (stopRequested) return false
        if (!running) return false
        if (lag() > 0) return false
        return true
    }

    override suspend fun lag(): Long {
        return delegateMutex.withLock {
            check(isRunning()) { "Lag cannot be computing when the consumer isn't running" }
            endOffsets.map { it.value - delegate.position(it.key) }.sumOf { it.coerceAtLeast(0) }
        }
    }

    override fun stop() {
        stopRequested = true
    }

    override fun close() {
        stop()
    }

    override suspend fun commit(commitOffsets: Map<TopicPartition, OffsetAndMetadata>) {
        if (commitOffsets.isEmpty()) return
        delegateMutex.withLock {
            check(isRunning()) { "Cannot commit transaction when the consumer isn't running" }
            delegate.commitAsync(commitOffsets) { offsets, exception ->
                if (exception != null) {
                    logger.warn("Error while committing offsets ($offsets)", exception)
                }
            }
        }
    }

    override suspend fun rollback(topicPartitionToRollback: Set<TopicPartition>) {
        if (topicPartitionToRollback.isEmpty()) return
        delegateMutex.withLock {
            check(isRunning()) { "Cannot rollback transaction when the consumer isn't running" }
            val committedOffsets = delegate.committed(topicPartitionToRollback)
            topicPartitionToRollback.forEach { topicPartition ->
                delegate.seek(topicPartition, committedOffsets[topicPartition]?.offset() ?: 0)
            }
        }
    }

    private suspend fun createConsumerChannel(): Channel<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>> {
        val channel = Channel<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>()
        CoroutineScope(currentCoroutineContext()).launch(Dispatchers.IO) {
            try {
                channel.send(StartConsuming(this@KafkaFlowConsumerWithGroupIdImpl))
                while (!shouldStop()) {
                    fetchAndProcessRecords(channel)
                }
                channel.close()
            } catch (t: CancellationException) {
            } catch (t: Throwable) {
                t.printStackTrace()
            } finally {
                channel.close()
            }
        }
        return channel
    }

    private suspend fun fetchAndProcessRecords(channel: Channel<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>) {
        val records = delegateMutex.withLock { delegate.poll(pollDuration) }
        partitionChangedMessages.forEach { channel.send(it) }
        partitionChangedMessages.clear()
        records.map { Record(it, Unit, Unit, Unit, Instant.ofEpochMilli(it.timestamp()), Unit, WithoutTransaction) }.forEach { channel.send(it) }
        if (records.isEmpty) yield()
        if (!records.isEmpty) channel.send(EndOfBatch())
    }

    private suspend fun shouldStop(): Boolean {
        if (stopRequested) return true
        return when (autoStopPolicy) {
            AutoStopPolicy.Never -> return false
            AutoStopPolicy.WhenUpToDate -> isUpToDate()
            is AutoStopPolicy.AtSpecificTime -> autoStopPolicy.stopTime < Instant.now() && isUpToDate()
            is AutoStopPolicy.SpecificOffsetFromNow -> startInstant!! + autoStopPolicy.duration < Instant.now() && isUpToDate()
        }
    }

    private suspend fun subscribe() {
        startInstant = Instant.now()
        delegateMutex.withLock {
            delegate.subscribe(topics, object : ConsumerRebalanceListener {
                override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) = partitionRevoked(partitions.toList())
                override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) = partitionAssigned(partitions.toList())
            })
        }
        running = true
        startEndOffsetsRefreshLoop()
    }

    private fun startEndOffsetsRefreshLoop() {
        CoroutineScope(EmptyCoroutineContext).launch(Dispatchers.IO) {
            val endOffsetConsumer: KafkaConsumer<ByteArray, ByteArray> = KafkaConsumer(properties, ByteArrayDeserializer(), ByteArrayDeserializer())
            endOffsetConsumer.use {
                while (!shouldStop()) {
                    try {
                        endOffsets = endOffsetConsumer.endOffsets(assignment)
                        delay(10.seconds().toMillis())
                    } catch (t: Throwable) {
                        logger.warn("Error while trying to fetch the end offsets", t)
                    }
                }
            }
        }
    }

    private fun partitionAssigned(assignedPartitions: List<TopicPartition>) {
        assignment = delegate.assignment().toList()
        seek(assignedPartitions)
        partitionChangedMessages.add(PartitionsAssigned(assignedPartitions, assignment))
    }

    private fun partitionRevoked(revokedPartition: List<TopicPartition>) {
        assignment = delegate.assignment().toList()
        partitionChangedMessages.add(PartitionsRevoked(revokedPartition, assignment))
    }

    private fun seek(assignedPartitions: List<TopicPartition>) {
        when (startOffsetPolicy) {
            is StartOffsetPolicy.SpecificOffsetFromNow -> seekToSpecifiedTime(assignedPartitions, Instant.now() - startOffsetPolicy.duration)
            is StartOffsetPolicy.SpecificTime -> seekToSpecifiedTime(assignedPartitions, startOffsetPolicy.offsetTime)
            StartOffsetPolicy.Earliest, StartOffsetPolicy.Latest -> {
                // Let the kafka internal client deal with that
            }
        }
    }

    private fun seekToSpecifiedTime(assignedPartitions: List<TopicPartition>, instant: Instant) {
        val endOffsets = delegate.endOffsets(assignedPartitions)
        val offsetsForTime = delegate.offsetsForTimes(assignedPartitions.associateWith { instant.toEpochMilli() })
        assignedPartitions.forEach {
            delegate.seek(it, offsetsForTime[it]?.offset() ?: endOffsets[it] ?: 0)
        }
    }

    private suspend fun FlowCollector<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>.cleanup() {
        try {
            emit(StopConsuming())
        } finally {
            CoroutineScope(EmptyCoroutineContext).launch(Dispatchers.IO) {
                delay(50)
                delegateMutex.withLock {
                    running = false
                    delegate.close()
                }
            }
        }
    }

    private fun properties(clientProperties: Properties): Properties {
        val properties = Properties()
        properties.putAll(clientProperties)
        if (startOffsetPolicy is StartOffsetPolicy.Earliest) properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        if (startOffsetPolicy is StartOffsetPolicy.Latest) properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
        return properties
    }

}