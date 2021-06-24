package kafka.flow.utils

import java.time.Duration

public fun Number.day(): Duration = this.days()
public fun Number.days(): Duration = when (this) {
    is Double, is Float -> Duration.ofDays(this.toLong()).plusNanos((1.day().toNanos() * this.toDouble().mod(1.0)).toLong())
    else -> Duration.ofDays(this.toLong())
}

public fun Number.hour(): Duration = this.hours()
public fun Number.hours(): Duration = when (this) {
    is Double, is Float -> Duration.ofHours(this.toLong()).plusNanos((1.hour().toNanos() * this.toDouble().mod(1.0)).toLong())
    else -> Duration.ofHours(this.toLong())
}

public fun Number.minute(): Duration = this.minutes()
public fun Number.minutes(): Duration = when (this) {
    is Double, is Float -> Duration.ofMinutes(this.toLong()).plusNanos((1.minute().toNanos() * this.toDouble().mod(1.0)).toLong())
    else -> Duration.ofMinutes(this.toLong())
}

public fun Number.second(): Duration = this.seconds()
public fun Number.seconds(): Duration = when (this) {
    is Double, is Float -> Duration.ofSeconds(this.toLong()).plusNanos((1.second().toNanos() * this.toDouble().mod(1.0)).toLong())
    else -> Duration.ofSeconds(this.toLong())
}

public fun Number.millisecond(): Duration = this.milliseconds()
public fun Number.milliseconds(): Duration = when (this) {
    is Double, is Float -> Duration.ofMillis(this.toLong()).plusNanos((1.millisecond().toNanos() * this.toDouble().mod(1.0)).toLong())
    else -> Duration.ofMillis(this.toLong())
}

public fun Number.nanosecond(): Duration = this.nanoseconds()
public fun Number.nanoseconds(): Duration = Duration.ofNanos(this.toLong())