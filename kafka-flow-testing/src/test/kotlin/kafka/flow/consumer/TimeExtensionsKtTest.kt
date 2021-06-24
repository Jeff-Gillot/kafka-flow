package kafka.flow.consumer

import junit.framework.TestCase
import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.time.Duration

class TimeExtensionsKtTest  {
    @Test
    fun testFloatConversion() {
        expectThat(1.2.seconds()).isEqualTo(Duration.ofSeconds(1).plusMillis(200))
    }

    @Test
    fun testLongConversion() {
        expectThat(1200L.seconds()).isEqualTo(Duration.ofSeconds(1200))
    }
}