package kafka.flow

import be.delta.flow.time.millisecond
import be.delta.flow.time.milliseconds
import be.delta.flow.time.nanoseconds
import be.delta.flow.time.seconds
import com.codahale.metrics.Meter
import com.codahale.metrics.Timer
import invokeAndThrow
import java.text.DecimalFormat
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kafka.flow.consumer.KafkaFlowConsumer
import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.Record
import kafka.flow.consumer.onEachRecord
import kafka.flow.consumer.onEndOfBatch
import kafka.flow.consumer.onPartitionAssigned
import kafka.flow.consumer.onPartitionRevoked
import kafka.flow.consumer.onStartConsuming
import kafka.flow.consumer.onStopConsuming
import kafka.flow.consumer.processor.Sink
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.producer.KafkaOutput
import kafka.flow.utils.FlowDebouncer
import kafka.flow.utils.FlowDebouncer.Companion.debounce
import kafka.flow.utils.logger
import kotlin.system.measureNanoTime
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

@Suppress("MemberVisibilityCanBePrivate")
public class KafkaMetricLogger(private val name: String) {
    private val _inputMeters = ConcurrentHashMap<String, Meter>()
    private val _outputMeters = ConcurrentHashMap<String, Meter>()
    private val _skippedMeters = ConcurrentHashMap<String, Meter>()
    private val _timers = ConcurrentHashMap<String, Timer>()
    private var consumer: KafkaFlowConsumer<*>? = null
    private val logger = logger()

    public val inputMeters: Map<String, Meter> = _inputMeters
    public val outputMeters: Map<String, Meter> = _outputMeters
    public val skippedMeters: Map<String, Meter> = _skippedMeters
    public val timers: Map<String, Timer> = _timers

    public suspend fun start(consumer: KafkaFlowConsumer<*>, interval: Duration = 30.seconds(), printBlock: (KafkaMetricLogger) -> Unit) {
        this.consumer = consumer
        CoroutineScope(currentCoroutineContext()).launch {
            val intervalInMillis = interval.toMillis()
            delay(intervalInMillis)
            while (consumer.isRunning() && isActive) {
                try {
                    printBlock.invokeAndThrow(this@KafkaMetricLogger)
                    delay(intervalInMillis)
                } catch (cancellationException: CancellationException) {
                } catch (throwable: Throwable) {
                    logger.error("Error while logging metrics", throwable)
                }
            }
        }
    }

    public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> registerInput(record: Record<Key, Partition, Value, Output, Transaction>, time: Long) {
        _inputMeters
            .computeIfAbsent(record.consumerRecord.topic()) { Meter() }
            .mark()

        _timers
            .computeIfAbsent(record.consumerRecord.topic()) { Timer() }
            .update(time.nanoseconds())
    }

    public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> registerSkip(record: Record<Key, Partition, Value, Output, Transaction>) {
        _inputMeters
            .computeIfAbsent(record.consumerRecord.topic()) { Meter() }
            .mark()

        _skippedMeters
            .computeIfAbsent(record.consumerRecord.topic()) { Meter() }
            .mark()
    }

    public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> registerInput(records: List<Record<Key, Partition, Value, Output, Transaction>>, time: Long) {
        records
            .groupBy { it.consumerRecord.topic() }
            .forEach { (topic, records) ->
                _inputMeters
                    .computeIfAbsent(topic) { Meter() }
                    .mark(records.size.toLong())
            }

        _timers
            .computeIfAbsent("mixed") { Timer() }
            .update((time.toDouble() / records.size).nanoseconds())
    }

    public fun registerOutput(output: KafkaOutput) {
        output.records.forEach { record ->
            _outputMeters
                .computeIfAbsent(record.topicDescriptor.name) { Meter() }
                .mark()
        }
    }

    public val prettyText: String
        get() = buildString {
            append(name)
            when (_inputMeters.size) {
                0 -> append(" No input yet")
                else -> {
                    _inputMeters.entries.forEach { (key, value) ->
                        val lag = computeLag(key)
                        val eta = computeEta(lag, value.fifteenMinuteRate)
                        val snapshot = _timers[key]?.snapshot ?: _timers["mixed"]?.snapshot
                        append(
                            "\r\tIN $key -> " +
                                    "${value.count.formatBigNumber()}, " +
                                    "1 min ${value.oneMinuteRate.formatted()}/s, " +
                                    "15 min ${value.fifteenMinuteRate.formatted()}/s, " +
                                    "processing: mean ${snapshot?.mean?.toMsString()}, " +
                                    "99% ${snapshot?.get99thPercentile()?.toMsString()}" +
                                    when (lag) {
                                        null -> ""
                                        else -> ", lag: ${lag.formatBigNumber()}"
                                    } +
                                    when {
                                        eta == null -> ""
                                        eta < 1.seconds() -> ", ETA: up to date"
                                        else -> ", ETA: $eta"
                                    }
                        )
                    }
                }
            }
            when (_outputMeters.size) {
                0 -> {
                    if (_inputMeters.size > 1) append("\r\t") else append(", ")
                    append("No output yet")
                }
                else -> {
                    _outputMeters.forEach { (key, value) ->
                        append(
                            "\r\tOUT $key -> " +
                                    "${value.count.formatBigNumber()}, " +
                                    "1 min ${value.oneMinuteRate.formatted()}/s, " +
                                    "15 min ${value.fifteenMinuteRate.formatted()}/s"
                        )
                    }
                }
            }
            _skippedMeters.forEach { (key, value) ->
                append(
                    "\r\tSKIPPED $key -> " +
                            "${value.count.formatBigNumber()}, " +
                            "1 min ${value.oneMinuteRate.formatted()}/s, " +
                            "15 min ${value.fifteenMinuteRate.formatted()}/s"
                )
            }
        }

    private fun computeLag(topic: String): Long? {
        val topicLag = consumer
            ?.lags()
            ?.filterKeys { it.topic() == topic }
        if (topicLag == null || topicLag.values.contains(null)) return null
        return topicLag.values.filterNotNull().sum()
    }

    private fun computeEta(lag: Long?, rate: Double): Duration? {
        if (lag == null) return null
        return (lag.toDouble() / rate).toInt().seconds()
    }

    override fun toString(): String {
        return prettyText
    }

    private fun Double.formatted(): String = String.format("%,.2f", this)
    private fun Double.toMsString(): String = (this / 1.millisecond().toNanos().toDouble()).formatted() + " ms"
    private fun Number.formatBigNumber(): String {
        return when {
            this.toLong() > 1_000_000_000 -> formatter.format(this.toDouble() / 1_000_000_000) + "g"
            this.toLong() > 1_000_000 -> formatter.format(this.toDouble() / 1_000_000) + "m"
            this.toLong() > 1_000 -> formatter.format(this.toDouble() / 1_000) + "k"
            this is Double || this is Float -> formatter.format(this.toDouble())
            else -> this.toString()
        }
    }

    public companion object {
        private val formatter = DecimalFormat("0.00")


        public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<List<KafkaMessage<Key, Partition, Value, Unit, Transaction>>>.mapValuesToOutput(
            kafkaMetricLogger: KafkaMetricLogger,
            block: suspend (List<Pair<Key, Value>>) -> Output
        ): Flow<Pair<List<KafkaMessage<Key, Partition, Value, Unit, Transaction>>, Output>> {
            return map { messages ->
                val records = messages.filterIsInstance<Record<Key, Partition, Value, Unit, Transaction>>()
                val keyValues = records.map { it.key to it.value }

                var result: Output
                val time = measureNanoTime {
                    result = block.invokeAndThrow(keyValues)
                }

                kafkaMetricLogger.registerInput(records, time)

                val output = result
                if (output is KafkaOutput) {
                    kafkaMetricLogger.registerOutput(output)
                }

                messages to output
            }
        }

        @Suppress("UNCHECKED_CAST")
        public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Unit, Transaction>>.mapValueToOutput(
            kafkaMetricLogger: KafkaMetricLogger,
            block: suspend (Key, Value) -> Output
        ): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
            return map { record ->
                if (record is Record) {
                    var result: Output
                    val time = measureNanoTime {
                        result = block.invokeAndThrow(record.key, record.value)
                    }

                    kafkaMetricLogger.registerInput(record, time)

                    val output = result
                    if (output is KafkaOutput) {
                        kafkaMetricLogger.registerOutput(output)
                    }

                    Record(record.consumerRecord, record.key, record.partitionKey, record.value, record.timestamp, output, record.transaction)
                } else {
                    record as KafkaMessage<Key, Partition, Value, Output, Transaction>
                }
            }
        }

        public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.debounceInputOnKey(
            kafkaMetricLogger: KafkaMetricLogger,
            timeProvider: (Record<Key, Partition, Value, Output, Transaction>, Instant?) -> Instant?,
            maxDebounceDuration: Duration,
            interval: Duration = 10.milliseconds(),
            cleanUpInterval: Duration = 10.seconds(),
        ): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
            return debounce(
                { message -> if (message is Record<Key, Partition, Value, Output, Transaction>) Pair(message.consumerRecord.topic(), message.key) else null },
                { message, instant -> if (message is Record<Key, Partition, Value, Output, Transaction>) timeProvider.invokeAndThrow(message, instant) else null },
                maxDebounceDuration,
                interval,
                cleanUpInterval
            ).mapNotNull { action ->
                if (action is FlowDebouncer.Skip<KafkaMessage<Key, Partition, Value, Output, Transaction>>) {
                    if (action.data is Record<Key, Partition, Value, Output, Transaction>) {
                        val record = action.data as Record<Key, Partition, Value, Output, Transaction>
                        record.transaction.unlock()
                        kafkaMetricLogger.registerSkip(record)
                    }
                    null
                } else {
                    action.data
                }
            }
        }

        public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.collect(
            kafkaMetricLogger: KafkaMetricLogger,
            processor: Sink<Key, Partition, Value, Output, Transaction>
        ) {
            return this
                .onStartConsuming(processor::startConsuming)
                .onStopConsuming(processor::stopConsuming)
                .onCompletion { processor.completion() }
                .onEndOfBatch(processor::endOfBatch)
                .onPartitionAssigned(processor::partitionAssigned)
                .onPartitionRevoked(processor::partitionRevoked)
                .onEachRecord { record ->
                    val time = measureNanoTime {
                        processor.record(record.consumerRecord, record.key, record.partitionKey, record.value, record.timestamp, record.output, record.transaction)
                    }
                    kafkaMetricLogger.registerInput(record, time)
                }
                .collect()
        }
    }
}