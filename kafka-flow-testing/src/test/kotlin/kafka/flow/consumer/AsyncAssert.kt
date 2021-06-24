package kafka.flow.consumer

import kotlinx.coroutines.delay
import java.time.Duration
import java.time.Instant

class Await() {
    var timeout = 10.seconds()
    var interval = 10.milliseconds()

    constructor(block: Await.() -> Unit) : this() {
        block.invoke(this)
    }

    fun timeout(duration: Duration): Await {
        timeout = duration
        return this
    }

    fun atMost(duration: Duration): Await {
        timeout = duration
        return this
    }

    suspend fun untilAsserted(block: () -> Unit) {
        val timeoutTime = Instant.now() + timeout
        var lastError: Throwable? = safeInvoke(block)
        while (lastError != null && timeoutTime > Instant.now()) {
            lastError = safeInvoke(block)
            delay(interval.toMillis())
        }
        lastError?.let { throw it }
    }

    private fun safeInvoke(block: () -> Unit): Throwable? = try {
        block.invoke()
        null
    } catch (error: Throwable) {
        error
    }

    companion object {
        fun await() = Await()
    }
}