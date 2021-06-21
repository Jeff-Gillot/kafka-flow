package be.delta.flow.client.with.group.id

import be.delta.flow.client.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.coroutines.EmptyCoroutineContext

public class KafkaFlowClientWithGroupId(
    private val clientProperties: Properties,
    private val topics: List<Topic>,
    private val startOffsetPolicy: StartOffsetPolicy,
    private val autoStopPolicy: AutoStopPolicy
) {
    private val delegate: KafkaConsumer<ByteArray, ByteArray> = KafkaConsumer(clientProperties, ByteArrayDeserializer(), ByteArrayDeserializer())
    private var running = false
    private var stopRequested: Boolean = false
    private var startInstant: Instant? = null
    private var endOffsets: Map<TopicPartition, Long> = emptyMap()
    private var assignment: List<TopicPartition> = emptyList()
    private val partitionChangedMessages = mutableListOf<PartitionChangedMessage>()
    private val delegateMutex = Mutex()

    public suspend fun startConsuming(): Flow<KafkaMessage<Unit, Unit, Unit, Unit>> = flow {
        subscribe()
        emit(StartConsuming)
        while (!shouldStop()) {
            val records = delegateMutex.withLock { delegate.poll(Duration.ZERO) }
            partitionChangedMessages.forEach { emit(it) }
            partitionChangedMessages.clear()
            records.map { Record(it, Unit, Unit, Unit, Unit) }.forEach { emit(it) }
            if (records.isEmpty) delay(10)
            if (!records.isEmpty) emit(EndOfBatch)
        }
        emit(StopConsuming)
    }.onCompletion {
        delegateMutex.withLock { delegate.close() }
        running = false
    }

    private suspend fun shouldStop(): Boolean {
        if (stopRequested) return true
        return when (autoStopPolicy) {
            AutoStopPolicy.Never -> return false
            AutoStopPolicy.WhenUpToDate -> isUpToDate()
            is AutoStopPolicy.AtSpecificTime -> autoStopPolicy.stopTime < Instant.now() && isUpToDate()
            is AutoStopPolicy.SpecificOffsetFromNow -> startInstant!! + autoStopPolicy.duration > Instant.now() && isUpToDate()
        }
    }

    public suspend fun isUpToDate(): Boolean {
        if (stopRequested) return false
        if (!running) return false
        if (lag() > 0) return false
        return true
    }

    public suspend fun lag(): Long {
        return delegateMutex.withLock {
            endOffsets.map { it.value - delegate.position(it.key) }.sumOf { it.coerceAtLeast(0) }
        }
    }

    private fun subscribe() {
        startInstant = Instant.now()
        val topicNames = topics.map { it.name }
        delegate.subscribe(topicNames, object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) = partitionRevoked(partitions.toList())
            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) = partitionAssigned(partitions.toList())
        })
        running = true
        startEndOffsetsRefreshLoop()
    }

    private fun startEndOffsetsRefreshLoop() {
        CoroutineScope(EmptyCoroutineContext).launch(Dispatchers.IO) {
            val endOffsetConsumer: KafkaConsumer<ByteArray, ByteArray> = KafkaConsumer(clientProperties)
            while (running) {
                try {
                    endOffsets = endOffsetConsumer.endOffsets(assignment)
                    delay(10_000)
                } catch (t: Throwable) {
                    println("Error while trying to fetch the end offsets")
                } finally {
                    endOffsetConsumer.close()
                }
            }
        }
    }

    private fun partitionAssigned(assignedPartitions: List<TopicPartition>) {
        runBlocking {
            assignment = delegateMutex.withLock { delegate.assignment().toList() }
            partitionChangedMessages.add(PartitionsAssigned(assignedPartitions, assignment))
            seek(assignedPartitions)
        }
    }

    private  fun partitionRevoked(revokedPartition: List<TopicPartition>) {
        runBlocking {
            assignment = delegateMutex.withLock { delegate.assignment().toList() }
            partitionChangedMessages.add(PartitionsRevoked(revokedPartition, assignment))
        }
    }

    private suspend fun seek(assignedPartitions: List<TopicPartition>) {
        when (startOffsetPolicy) {
            is StartOffsetPolicy.SpecificOffsetFromNow -> seekToSpecifiedTime(assignedPartitions, Instant.now() - startOffsetPolicy.duration)
            is StartOffsetPolicy.SpecificTime -> seekToSpecifiedTime(assignedPartitions, startOffsetPolicy.offsetTime)
            StartOffsetPolicy.Earliest, StartOffsetPolicy.Latest -> {
                // Let the kafka internal client deal with that
            }
        }
    }

    private suspend fun seekToSpecifiedTime(assignedPartitions: List<TopicPartition>, instant: Instant) = delegateMutex.withLock {
        val endOffsets = delegate.endOffsets(assignedPartitions)
        val offsetsForTime = delegate.offsetsForTimes(assignedPartitions.associateWith { instant.toEpochMilli() })
        assignedPartitions.forEach {
            delegate.seek(it, offsetsForTime[it]?.offset() ?: endOffsets[it] ?: 0)
        }
    }
}