package kafka.flow.utils

import be.delta.flow.time.milliseconds
import be.delta.flow.time.second
import java.time.Instant
import kafka.flow.utils.FlowDebouncer.Companion.debounceAndIgnoreSkip
import kotlin.test.Test
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import strikt.api.expectThat
import strikt.assertions.containsExactly
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThan
import strikt.assertions.isLessThan

class FlowDebouncerTest {
    @Test
    fun `three in, one out`() = run {
        val flow = flowOf(1, 2, 3)

        val result = flow
            .debounceAndIgnoreSkip({ 1 }, { _, _ -> Instant.now() + 10.milliseconds() }, 10.milliseconds())
            .toList()

        expectThat(result).containsExactly(3)
    }

    @Test
    fun `three in, three out`() = run {
        val flow = flowOf(1, 2, 3)

        val result = flow
            .debounceAndIgnoreSkip({ it }, { _, _ -> Instant.now() + 10.milliseconds() }, 10.milliseconds())
            .toList()

        expectThat(result).containsExactlyInAnyOrder(1, 2, 3)
    }


    @Test
    fun `six in, two out`() = run {
        val flow = flowOf("a" to 1, "b" to 1, "b" to 2, "a" to 2, "a" to 3, "b" to 3)

        val result = flow
            .debounceAndIgnoreSkip({ it.first }, { _, _ -> Instant.now() + 10.milliseconds() }, 10.milliseconds())
            .toList()

        expectThat(result).containsExactlyInAnyOrder("a" to 3, "b" to 3)
    }

    @Test
    fun `six in, six out no debouncing`() = run {
        val flow = flowOf("a" to 1, "b" to 1, "b" to 2, "a" to 2, "a" to 3, "b" to 3)

        val result = flow
            .debounceAndIgnoreSkip({ it.first }, { _, _ -> null }, 10.milliseconds())
            .toList()

        expectThat(result).containsExactly("a" to 1, "b" to 1, "b" to 2, "a" to 2, "a" to 3, "b" to 3)
    }

    @Test
    fun `six in, two out, takes 1 second`() = run {
        val flow = flow {
            emit("a" to 1)
            delay(50)
            emit("a" to 2)
            delay(50)
            emit("a" to 3)
            delay(1200)
        }

        val startTime = Instant.now()

        val result = flow
            .debounceAndIgnoreSkip({ it.first }, { _, _ -> Instant.now() + 1.second() }, 10.milliseconds())
            .map { it to Instant.now()!! }
            .toList()

        expectThat(result).hasSize(1)
        expectThat(result[0]) {
            this.get { first }.isEqualTo("a" to 3)
            this.get { second }.isGreaterThan(startTime + 1.second())
        }
    }

    @Test
    fun `six in, two out, takes less than 1 second`() = run {
        val flow = flow {
            emit("a" to 1)
            delay(50)
            emit("a" to 2)
            delay(50)
            emit("a" to 3)
            delay(1200)
        }

        val startTime = Instant.now()

        val result = flow
            .debounceAndIgnoreSkip({ it.first }, { _, _ -> Instant.now() + 100.milliseconds() }, 10.milliseconds())
            .map { it to Instant.now()!! }
            .toList()

        expectThat(result).hasSize(1)
        expectThat(result[0]) {
            this.get { first }.isEqualTo("a" to 3)
            this.get { second }.isLessThan(startTime + 1.second())
        }
    }

    private fun run(block: suspend () -> Unit) {
        runBlocking {
            block.invoke()
        }
    }
}