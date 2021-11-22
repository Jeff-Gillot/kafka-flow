package kafka.flow

import be.delta.flow.time.millisecond
import be.delta.flow.time.nanoseconds
import be.delta.flow.time.seconds
import com.codahale.metrics.Meter
import com.codahale.metrics.Timer
import java.text.DecimalFormat
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kafka.flow.consumer.KafkaFlowConsumer
import kafka.flow.consumer.Record
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.producer.KafkaOutput
import kafka.flow.utils.logger
import kotlin.coroutines.EmptyCoroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

@Suppress("MemberVisibilityCanBePrivate")
public class KafkaMetricLogger(private val name: String) {
    private val _inputMeters = ConcurrentHashMap<String, Meter>()
    private val _outputMeters = ConcurrentHashMap<String, Meter>()
    private val _timers = ConcurrentHashMap<String, Timer>()
    private val logger = logger()

    public val inputMeters: Map<String, Meter> = _inputMeters
    public val outputMeters: Map<String, Meter> = _outputMeters
    public val timers: Map<String, Timer> = _timers

    public fun start(consumer: KafkaFlowConsumer<*>, interval: Duration = 30.seconds(), printBlock: (KafkaMetricLogger) -> Unit) {
        CoroutineScope(EmptyCoroutineContext).launch {
            val intervalInMillis = interval.toMillis()
            while (consumer.isRunning() && isActive) {
                try {
                    delay(intervalInMillis)
                    printBlock.invoke(this@KafkaMetricLogger)
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
                        val snapshot = _timers[key]?.snapshot ?: _timers["mixed"]?.snapshot
                        append(
                            "\r\tIN $key -> " +
                                    "${value.count.formatBigNumber()}, " +
                                    "1 min ${value.oneMinuteRate.formatted()}/s, " +
                                    "15 min ${value.fifteenMinuteRate.formatted()}/s, " +
                                    "processing: mean ${snapshot?.mean?.toMsString()}, " +
                                    "99% ${snapshot?.get99thPercentile()?.toMsString()}"
                        )
                    }
                }
            }
            when (_outputMeters.size) {
                0 -> {
                    if (_inputMeters.size > 1) {
                        append("\r\t")
                    } else {
                        append(", ")
                    }
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
        }

    override fun toString(): String {
        return prettyText
    }

    private fun Double.formatted(): String = String.format("%,.2f", this)
    private fun Double.toMsString(): String = (this / 1.millisecond().toNanos().toDouble()).formatted() + " ms"
    private fun Number.formatBigNumber(): String {
        return when {
            this.toLong() > 1_000_000_000 -> formatter.format(this.toDouble() / 1_000_000_000) + "b"
            this.toLong() > 1_000_000 -> formatter.format(this.toDouble() / 1_000_000) + "m"
            this.toLong() > 1_000 -> formatter.format(this.toDouble() / 1_000) + "k"
            this is Double || this is Float -> formatter.format(this.toDouble())
            else -> this.toString()
        }
    }

    private companion object {
        private val formatter = DecimalFormat("0.00");
    }
}