package kafka.flow.consumer.with.group.id

import be.delta.flow.time.milliseconds
import be.delta.flow.time.seconds
import java.time.Instant
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import kafka.flow.consumer.AutoStopPolicy
import kafka.flow.consumer.EndOfBatch
import kafka.flow.consumer.KafkaFlowConsumerWithGroupId
import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.PartitionChangedMessage
import kafka.flow.consumer.PartitionsAssigned
import kafka.flow.consumer.PartitionsRevoked
import kafka.flow.consumer.Record
import kafka.flow.consumer.StartConsuming
import kafka.flow.consumer.StartOffsetPolicy
import kafka.flow.consumer.StopConsuming
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.yield
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.LoggerFactory

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
    private var _assignment: List<TopicPartition> = emptyList()
    private val partitionChangedMessages = mutableListOf<PartitionChangedMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>()
    private val delegateMutex = Mutex()
    private val pollDuration = 10.milliseconds()
    private val positions = ConcurrentHashMap<TopicPartition, Long>()
    override val assignment: List<TopicPartition> get() = _assignment

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

    override fun isUpToDate(): Boolean {
        if (stopRequested) return false
        if (!running) return false
        val lag = lag()
        if (lag == null || lag > 0) return false
        return true
    }

    override fun lags(): Map<TopicPartition, Long?>? {
        if (!isRunning()) return null
        if (_assignment.isEmpty()) return emptyMap()
        return _assignment.associateWith {
            val endOffset = endOffsets[it]
            val position = positions[it]
            if (position != null && endOffset != null) {
                (endOffset - position).coerceAtLeast(0)
            } else if (endOffset == 0L) {
                0L
            } else {
                null
            }
        }
    }

    override fun lag(): Long? {
        val lags: Map<TopicPartition, Long?>? = lags()
        if (lags == null || lags.values.contains(null)) return null
        return lags.values.filterNotNull().sum()
    }

    override fun stop() {
        stopRequested = true
    }

    override fun close() {
        stop()
    }

    override suspend fun commit(offsetsToCommit: Map<TopicPartition, OffsetAndMetadata>): Result<Map<TopicPartition, OffsetAndMetadata>> {
        val mutex = Mutex(true)
        var result: Result<Map<TopicPartition, OffsetAndMetadata>>? = null

        if (!isRunning()) {
            result = Result.success(emptyMap())
        } else {
            delegateMutex.withLock {
                try {
                    delegate.commitAsync(offsetsToCommit) { offsets, exception ->
                        result = if (exception != null) {
                            Result.failure(exception)
                        } else {
                            Result.success(offsets!!)
                        }
                        mutex.unlock()
                    }
                } catch (throwable: Throwable) {
                    result = Result.failure(throwable)
                    mutex.unlock()
                }
            }
        }
        mutex.lock()

        return result!!
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
                while (!shouldStop() && isActive) {
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
        delegateMutex.withLock {
            _assignment.forEach { topicPartition ->
                runCatching { delegate.position(topicPartition) }
                    .onSuccess { positions[topicPartition] = it }
                    .onFailure { positions.remove(topicPartition) }
            }
        }
        if (records.isEmpty) yield()
        if (!records.isEmpty) channel.send(EndOfBatch())
    }

    private fun shouldStop(): Boolean {
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

    private suspend fun startEndOffsetsRefreshLoop() {
        CoroutineScope(currentCoroutineContext()).launch(Dispatchers.IO) {
            val endOffsetConsumer: KafkaConsumer<ByteArray, ByteArray> = KafkaConsumer(properties, ByteArrayDeserializer(), ByteArrayDeserializer())
            endOffsetConsumer.use {
                while (!shouldStop() && isActive) {
                    try {
                        endOffsets = endOffsetConsumer.endOffsets(_assignment)
                        delay(10.seconds().toMillis())
                    } catch (t: Throwable) {
                        logger.warn("Error while trying to fetch the end offsets", t)
                    }
                }
            }
        }
    }

    private fun partitionAssigned(assignedPartitions: List<TopicPartition>) {
        _assignment = delegate.assignment().toList()
        seek(_assignment)
        partitionChangedMessages.add(PartitionsAssigned(assignedPartitions, _assignment))
    }

    private fun partitionRevoked(revokedPartition: List<TopicPartition>) {
        _assignment = delegate.assignment().toList()
        seek(_assignment)
        partitionChangedMessages.add(PartitionsRevoked(revokedPartition, _assignment))
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
            CoroutineScope(currentCoroutineContext()).launch(Dispatchers.IO) {
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