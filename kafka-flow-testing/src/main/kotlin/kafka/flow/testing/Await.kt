package kafka.flow.testing

import be.delta.flow.time.milliseconds
import be.delta.flow.time.seconds
import invokeAndThrow
import kotlinx.coroutines.delay
import java.time.Duration
import java.time.Instant

public class Await() {
    public var timeout: Duration = 10.seconds()
    public var interval: Duration = 10.milliseconds()

    public constructor(block: Await.() -> Unit) : this() {
        block.invokeAndThrow(this)
    }

    public fun timeout(duration: Duration): Await {
        timeout = duration
        return this
    }

    public fun atMost(duration: Duration): Await {
        timeout = duration
        return this
    }

    public suspend fun untilAsserted(block: suspend () -> Unit) {
        val timeoutTime = Instant.now() + timeout
        var lastError: Throwable? = safeInvoke(block)
        while (lastError != null && timeoutTime > Instant.now()) {
            lastError = safeInvoke(block)
            delay(interval.toMillis())
        }
        lastError?.let { throw it }
    }

    private suspend fun safeInvoke(block: suspend () -> Unit): Throwable? = try {
        block.invoke()
        null
    } catch (error: Throwable) {
        error
    }
}