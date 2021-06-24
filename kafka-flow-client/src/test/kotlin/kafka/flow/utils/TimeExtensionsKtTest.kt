package kafka.flow.utils

import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.time.Duration

class TimeExtensionsKtTest {
    @Test
    fun testFloatConversion() {
        expectThat(1.5.seconds()).isEqualTo(Duration.ofSeconds(1).plusMillis(500))
    }

    @Test
    fun testLongConversion() {
        expectThat(1200L.seconds()).isEqualTo(Duration.ofSeconds(1200))
    }

    @Test
    fun testHours() {
        expectThat(1.5.hours()).isEqualTo(Duration.ofMinutes(90))
    }

    @Test
    fun testDays() {
        expectThat(1.5.days()).isEqualTo(Duration.ofHours(36))
    }

    @Test
    fun testSum() {
        expectThat(1.day() + 3.hours()).isEqualTo(Duration.ofHours(27))
    }

    @Test
    fun testDayHourMinutesSecondsNanos() {
        expectThat(1.day() + 3.hours() + 6.minutes() + 5.seconds() + 200.milliseconds() + 300.nanoseconds())
            .isEqualTo(Duration.ofDays(1).plusHours(3).plusMinutes(6).plusSeconds(5).plusMillis(200).plusNanos(300))
    }
}
