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
import kafka.flow.consumer.StopConsuming
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kafka.flow.utils.logger
import kotlin.coroutines.EmptyCoroutineContext
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
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.yield
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

public class KafkaFlowConsumerWithoutGroupIdImpl(
    clientProperties: Properties,
    private val assignment: List<TopicPartition>,
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
    private val pollDuration = 10.milliseconds()
    private val positions = ConcurrentHashMap<TopicPartition, Long>()

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

    override fun lag(): Long? {
        if (!isRunning()) return null
        if (assignment.isEmpty()) return 0
        if (assignment.size != endOffsets.size) return null
        if (!endOffsets.keys.containsAll(assignment)) return null
        val lags = assignment.map {
            val endOffset = endOffsets[it]
            val position = positions[it]
            if (position != null && endOffset != null) {
                (endOffset - position).coerceAtLeast(0)
            } else if (endOffset == 0L) {
                0
            } else {
                null
            }
        }
        if (lags.contains(null)) return null
        return lags.filterNotNull().sum()
    }

    override fun stop() {
        stopRequested = true
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
        records.groupBy { TopicPartition(it.topic(), it.partition()) }.forEach { (topicPartition, records) ->
            val lastOffset = records.last().offset()
            positions.computeIfAbsent(topicPartition) { lastOffset + 1}
            positions.computeIfPresent(topicPartition) { _, _ -> lastOffset + 1}
        }
        records.map { Record(it, Unit, Unit, Unit, Instant.ofEpochMilli(it.timestamp()), Unit, WithoutTransaction) }.forEach { channel.send(it) }
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
        assignment.forEach { positions[it] = delegate.position(it) }
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
}