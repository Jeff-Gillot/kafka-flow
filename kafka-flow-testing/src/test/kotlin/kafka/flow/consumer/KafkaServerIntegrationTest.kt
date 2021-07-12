package kafka.flow.consumer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.runBlocking

abstract class KafkaServerIntegrationTest {
    fun runTest(block: suspend CoroutineScope.() -> Unit) {
        runBlocking {
            block.invoke(this)
            currentCoroutineContext().cancelChildren()
        }
    }
}