package kafka.flow.consumer

import java.time.Duration

fun Number.second(): Duration = this.seconds()
fun Number.seconds(): Duration = when (this) {
    is Double, is Float -> Duration.ofSeconds(this.toLong()).plusNanos(((this.toDouble() * 1_000_000_000) % 1_000_000_000).toLong())
    else -> Duration.ofSeconds(this.toLong())
}

fun Number.millisecond(): Duration = this.milliseconds()
fun Number.milliseconds(): Duration = when (this) {
    is Double, is Float -> Duration.ofNanos((this.toDouble() * 1_000_000).toLong())
    else -> Duration.ofMillis(this.toLong())
}