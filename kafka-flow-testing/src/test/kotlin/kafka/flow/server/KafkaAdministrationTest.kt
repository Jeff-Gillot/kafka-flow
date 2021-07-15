package kafka.flow.server

import kafka.flow.testing.TestServer
import kafka.flow.testing.TestTopicDescriptor
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.util.concurrent.TimeUnit


internal class KafkaAdministrationTest {
    @Test
    fun test() {
        val topic = TestTopicDescriptor.next()
        TestServer.admin().createTopics(topic)

        val retention= TestServer.admin().withAdminClient {
            describeConfigs(listOf(ConfigResource(ConfigResource.Type.TOPIC, topic.name))).all().get(30, TimeUnit.SECONDS).mapKeys { it.key.name() }[topic.name]
        }

        expectThat(retention?.get(TopicConfig.RETENTION_MS_CONFIG)?.value()).isEqualTo("300000")
    }
}