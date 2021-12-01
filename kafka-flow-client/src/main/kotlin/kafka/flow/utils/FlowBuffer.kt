package kafka.flow.utils

import java.time.Duration
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import kotlin.coroutines.cancellation.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.yield

public class FlowBuffer<T>(
    private val batchSize: Int,
    private val timeSpan: Duration
) {
    private val records = ArrayBlockingQueue<T>(batchSize)
    private var lastBatchTime = Instant.now()
    private val output = Channel<List<T>>()

    public suspend fun start(input: Flow<T>): Flow<List<T>> {
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
                    while (!records.offer(record)) {
                        delay(10)
                    }
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

    public companion object {
        public suspend fun <T> Flow<T>.batch(batchSize: Int, timeSpan: Duration): Flow<List<T>> = FlowBuffer<T>(batchSize, timeSpan).start(this)

        public fun <T> Flow<List<T>>.flatten(): Flow<T> = flow {
            collect { list ->
                list.forEach {
                    emit(it)
                }
            }
        }
    }
}