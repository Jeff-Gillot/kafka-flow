package kafka.flow.consumer

import java.util.concurrent.TimeoutException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

fun runTest(block: suspend CoroutineScope.() -> Unit) {
    runBlocking {
        launch {
            delay(120_000)
            throw TimeoutException("Test didn't finish in time")
        }
        try {
            block.invoke(this)
        } finally {
            currentCoroutineContext().cancelChildren()
        }
    }
}
