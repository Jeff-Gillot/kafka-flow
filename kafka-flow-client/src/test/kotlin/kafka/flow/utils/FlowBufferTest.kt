package kafka.flow.utils

import be.delta.flow.time.milliseconds
import be.delta.flow.time.second
import invokeAndThrow
import kafka.flow.utils.FlowBuffer.Companion.batch
import kotlin.test.Test
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import strikt.api.expectThat
import strikt.assertions.containsExactly


class FlowBufferTest {

    @Test
    fun batchBigEnough() = run {
        val result = flowOf(1, 2, 3, 4)
            .batch(10, 1.second())
            .first()

        expectThat(result).containsExactly(1, 2, 3, 4)
    }

    @Test
    fun batchTooSmall() = run {
        val result = flowOf(1, 2, 3, 4)
            .batch(2, 1.second())
            .toList()

        expectThat(result).containsExactly(listOf(1, 2), listOf(3, 4))
    }

    @Test
    fun hugeFlowSmallBatch() = run {
        val result = (1..1000)
            .asFlow()
            .batch(10, 1.second())
            .toList()

        val expected = (1..1000).windowed(10, 10)

        expectThat(result).containsExactly(expected)
    }


    @Test
    fun delayedBatch() = run {
        val result = flow {
            (1..4).map {
                delay(100)
                emit(it)
            }
        }.batch(10, 250.milliseconds())
            .toList()

        expectThat(result).containsExactly(listOf(1, 2), listOf(3, 4))
    }

    private fun run(block: suspend () -> Unit) {
        runBlocking {
            block.invokeAndThrow()
        }
    }
}