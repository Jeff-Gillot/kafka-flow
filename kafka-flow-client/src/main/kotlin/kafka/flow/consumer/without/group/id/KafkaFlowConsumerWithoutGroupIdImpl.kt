package kafka.flow.consumer.without.group.id

import be.delta.flow.time.milliseconds
import be.delta.flow.time.seconds
import java.time.Instant
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import kafka.flow.consumer.AutoStopPolicy
import kafka.flow.consumer.EndOfBatch
import kafka.flow.consumer.KafkaFlowConsumerWithoutGroupId
import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.PartitionsAssigned
import kafka.flow.consumer.Record
import kafka.flow.consumer.StartConsuming
import kafka.flow.consumer.StartOffsetPolicy
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kafka.flow.utils.logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

public class KafkaFlowConsumerWithoutGroupIdImpl(
    clientProperties: Properties,
    override val assignment: List<TopicPartition>,
    private val startOffsetPolicy: StartOffsetPolicy,
    private val autoStopPolicy: AutoStopPolicy
) : KafkaFlowConsumerWithoutGroupId<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>> {
    private val logger = logger()
    private val properties: Properties = Properties().apply { putAll(clientProperties) }
    private val delegate: KafkaConsumer<ByteArray, ByteArray> = KafkaConsumer(properties, ByteArrayDeserializer(), ByteArrayDeserializer())
    private var running = false
    private var stopRequested: Boolean = false
    private var startInstant: Instant? = null
    private var endOffsets: Map<TopicPartition, Long> = emptyMap()
    private val delegateMutex = Mutex()
    private val pollDuration = 100.milliseconds()
    private val positions = ConcurrentHashMap<TopicPartition, Long>()
    private var endOffsetsLoop: Job? = null

    init {
        require(clientProperties[ConsumerConfig.GROUP_ID_CONFIG] == null) { "${ConsumerConfig.GROUP_ID_CONFIG} must NOT be set" }
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
        if (assignment.isEmpty()) return emptyMap()
        return assignment.associateWith {
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
        endOffsetsLoop?.cancel()
    }

    override fun close() {
        stop()
    }

    private suspend fun createConsumerChannel(): Channel<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>> {
        val channel = Channel<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>()
        CoroutineScope(currentCoroutineContext()).launch(Dispatchers.IO) {
            try {
                channel.send(StartConsuming(this@KafkaFlowConsumerWithoutGroupIdImpl))
                channel.send(PartitionsAssigned(assignment, assignment))
                do {
                    fetchAndProcessRecords(channel)
                } while (!shouldStop() && isActive)
                channel.close()
            } catch (t: CancellationException) {
            } catch (t: Throwable) {
                t.printStackTrace()
            } finally {
                channel.close()
                endOffsetsLoop?.cancel()
            }
        }
        return channel
    }

    private suspend fun fetchAndProcessRecords(channel: Channel<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>) {
        val records = delegateMutex.withLock { delegate.poll(pollDuration) }
        records.map { Record(it, Unit, Unit, Unit, Instant.ofEpochMilli(it.timestamp()), Unit, WithoutTransaction) }.forEach { channel.send(it) }
        delegateMutex.withLock {
            assignment.forEach { topicPartition ->
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
            delegate.assign(assignment)
            seek()
        }
        running = true
        startEndOffsetsRefreshLoop()
    }

    private suspend fun startEndOffsetsRefreshLoop() {
        endOffsetsLoop = CoroutineScope(currentCoroutineContext()).launch(Dispatchers.IO) {
            val endOffsetConsumer: KafkaConsumer<ByteArray, ByteArray> = KafkaConsumer(properties, ByteArrayDeserializer(), ByteArrayDeserializer())
            endOffsetConsumer.use {
                while (!shouldStop() && isActive) {
                    try {
                        endOffsets = endOffsetConsumer.endOffsets(assignment)
                        delay(10.seconds().toMillis())
                    } catch (t: CancellationException) {
                    } catch (throwable: Throwable) {
                        logger.warn("Error while trying to fetch the end offsets", throwable)
                    }
                }
            }
        }
    }

    private fun seek() {
        when (startOffsetPolicy) {
            is StartOffsetPolicy.SpecificOffsetFromNow -> seekToSpecifiedTime(assignment, Instant.now() - startOffsetPolicy.duration)
            is StartOffsetPolicy.SpecificTime -> seekToSpecifiedTime(assignment, startOffsetPolicy.offsetTime)
            StartOffsetPolicy.Earliest -> delegate.seekToBeginning(assignment)
            StartOffsetPolicy.Latest -> delegate.seekToEnd(assignment)
        }
    }

    private fun seekToSpecifiedTime(assignedPartitions: List<TopicPartition>, instant: Instant) {
        val endOffsets = delegate.endOffsets(assignedPartitions)
        val offsetsForTime = delegate.offsetsForTimes(assignedPartitions.associateWith { instant.toEpochMilli() })
        assignedPartitions.forEach {
            delegate.seek(it, offsetsForTime[it]?.offset() ?: endOffsets[it] ?: 0)
        }
    }

    private suspend fun cleanup() {
        withContext(NonCancellable) {
            delay(250)
            delegate.close()
            running = false
        }
    }
}