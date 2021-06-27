package kafka.flow.consumer

import kafka.flow.server.KafkaServer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.runBlocking

abstract class KafkaServerIntegrationTest {
    val kafkaServer: KafkaServer

    init {
        TestKafkaServer.start()
        kafkaServer = TestKafkaServer.kafkaServer
    }

    fun runTest(block: suspend CoroutineScope.() -> Unit) {
        runBlocking {
            block.invoke(this)
            currentCoroutineContext().cancelChildren()
        }
    }
}