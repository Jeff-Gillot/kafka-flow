package kafka.flow.consumer.processor

import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.onCompletion
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue

public class BufferProcessor<Key, Partition, Value, Transaction : MaybeTransaction>(
    private val batchSize: Int,
    private val timeSpan: Duration
) {
    private val records = ConcurrentLinkedQueue<KafkaMessage<Key, Partition, Value, Unit, Transaction>>()
    private var lastBatchTime = Instant.now()
    private val output = Channel<List<KafkaMessage<Key, Partition, Value, Unit, Transaction>>>()

    public suspend fun start(input: Flow<KafkaMessage<Key, Partition, Value, Unit, Transaction>>): Flow<List<KafkaMessage<Key, Partition, Value, Unit, Transaction>>> {
        val timerJob = CoroutineScope(currentCoroutineContext()).launch {
            while (isActive) {
                do {
                    val timeToWait = Duration.between(Instant.now(), lastBatchTime + timeSpan)
                    if (timeToWait > Duration.ZERO) {
                        delay(timeToWait.toMillis())
                    } else {
                        yield()
                    }
                } while (timeToWait > Duration.ZERO)
                sendBatch()
            }
        }
        val inputReader = CoroutineScope(currentCoroutineContext()).launch {
            try {
                input.collect { record ->
                    records.add(record)
                    if (records.size >= batchSize) {
                        sendBatch()
                    }
                }
                sendRecords()
            } catch (cancellation: CancellationException) {
                output.close(cancellation)
            } finally {
                output.close()
            }
        }
        return output
            .consumeAsFlow()
            .onCompletion { cause: Throwable? ->
                timerJob.cancel(if (cause is CancellationException) cause else null)
                inputReader.cancel(if (cause is CancellationException) cause else null)
            }
    }

    private suspend fun sendBatch() {
        if (records.size >= batchSize || lastBatchTime + timeSpan < Instant.now()) {
            sendRecords()
        }
    }

    private suspend fun sendRecords() {
        val recordsToSend = (1..batchSize).map { records.poll() }.takeWhile { it != null }.filterNotNull()
        lastBatchTime = Instant.now()
        if (recordsToSend.isNotEmpty()) {
            output.send(recordsToSend)
        }
    }
}