package kafka.flow.consumer

import kafka.flow.server.KafkaServer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

object TestKafkaServer {
    private val kafkaContainer: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"))
    lateinit var kafkaServer: KafkaServer

    fun start() {
        kafkaContainer.start()
        kafkaServer = KafkaServer(kafkaContainer.bootstrapServers)
    }
}