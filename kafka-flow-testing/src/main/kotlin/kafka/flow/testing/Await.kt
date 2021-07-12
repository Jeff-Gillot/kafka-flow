package kafka.flow.testing

import be.delta.flow.time.milliseconds
import be.delta.flow.time.seconds
import kotlinx.coroutines.delay
import java.time.Duration
import java.time.Instant

public class Await() {
    public var timeout: Duration = 10.seconds()
    public var interval: Duration = 10.milliseconds()

    public constructor(block: Await.() -> Unit) : this() {
        block.invoke(this)
    }

    public fun timeout(duration: Duration): Await {
        timeout = duration
        return this
    }

    public fun atMost(duration: Duration): Await {
        timeout = duration
        return this
    }

    public suspend fun untilAsserted(block: () -> Unit) {
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
}