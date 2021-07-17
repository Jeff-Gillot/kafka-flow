package kafka.flow.server

import kafka.flow.TopicDescriptor
import kafka.flow.utils.logger
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.config.ConfigResource
import java.util.*
import java.util.concurrent.TimeUnit

public class KafkaAdministration(private val properties: Properties) {
    private val logger = logger()

    public fun <Key, Partition, Value> createTopics(vararg topicDescriptors: TopicDescriptor<Key, Partition, Value>): Unit = createTopics(topicDescriptors.toList())

    public fun createTopics(topicDescriptors: List<TopicDescriptor<*, *, *>>) {
        withAdminClient {
            val existingTopics = fetchAllExistingTopics()
            val existingTopicConfig = fetchAllTopicConfig(existingTopics)
            validatePartitionNumbersForExistingTopics(topicDescriptors, existingTopics)
            createTopicThatDoNotExist(topicDescriptors, existingTopics)
            updateTopicConfigIfNeeded(topicDescriptors, existingTopicConfig)
        }
    }

    private fun AdminClient.updateTopicConfigIfNeeded(topicDescriptors: List<TopicDescriptor<*, *, *>>, existingTopicConfig: Map<String, Config>) {
        val alterConfig = topicDescriptors.filter { existingTopicConfig.containsKey(it.name) }
            .filter { it.config.isNotEmpty() }
            .asSequence()
            .filter { !isSameConfig(it.config, existingTopicConfig[it.name]!!) }
            .onEach { logger.warn("Updating config for topic ${it.name}") }
            .associate { topic ->
                ConfigResource(ConfigResource.Type.TOPIC, topic.name) to
                        topic.config
                            .filter { it.value != existingTopicConfig[topic.name]!![it.key].value() }
                            .onEach { logger.warn("    ${it.key} -> ${it.value}") }
                            .map { AlterConfigOp(ConfigEntry(it.key, it.value), AlterConfigOp.OpType.SET) }
            }
        incrementalAlterConfigs(alterConfig).all().get(30, TimeUnit.SECONDS)
    }

    private fun isSameConfig(expected: Map<String, String>, existing: Config): Boolean {
        return expected.all { it.value == existing[it.key]?.value() }
    }

    private fun validatePartitionNumbersForExistingTopics(topicDescriptors: List<TopicDescriptor<*, *, *>>, currentTopics: Map<String, TopicDescription>) {
        topicDescriptors.forEach { topic ->
            val topicDescription = currentTopics[topic.name]
            if (topicDescription != null) {
                if (topic.partitionNumber != topicDescription.partitions().size) {
                    throw IllegalStateException("The number of partitions in the topic descriptor do not match the existing number of partition for the topic '${topic.name}' ${topicDescription.partitions()} vs ${topic.partitionNumber}")
                }
            }
        }
    }

    public fun <T> withAdminClient(block: AdminClient.() -> T): T {
        return AdminClient.create(properties).use { block.invoke(it) }
    }

    private fun AdminClient.fetchAllExistingTopics(): Map<String, TopicDescription> {
        val topics = listTopics().listings().get(30, TimeUnit.SECONDS).map { it.name() }.toList()
        return describeTopics(topics).all().get(30, TimeUnit.SECONDS)
    }

    private fun AdminClient.fetchAllTopicConfig(existingTopics: Map<String, TopicDescription>): Map<String, Config> {
        return describeConfigs(existingTopics.map { ConfigResource(ConfigResource.Type.TOPIC, it.key) }).all().get(30, TimeUnit.SECONDS).mapKeys { it.key.name() }
    }

    private fun AdminClient.createTopicThatDoNotExist(topicDescriptors: List<TopicDescriptor<*, *, *>>, currentTopics: Map<String, TopicDescription>) {
        val newTopics = topicDescriptors
            .filter { !currentTopics.containsKey(it.name) }
            .map { NewTopic(it.name, Optional.of(it.partitionNumber), Optional.empty()) }
            .onEach { logger.warn("Creating topic ${it.name()}") }
        createTopics(newTopics).all().get(30, TimeUnit.SECONDS)

        val alterConfig = topicDescriptors
            .filter { !currentTopics.containsKey(it.name) }
            .filter { it.config.isNotEmpty() }
            .asSequence()
            .onEach { logger.warn("Updating config for topic ${it.name}") }
            .associate { topic ->
                ConfigResource(ConfigResource.Type.TOPIC, topic.name) to
                        topic.config
                            .onEach { logger.warn("    ${it.key} -> ${it.value}") }
                            .map { AlterConfigOp(ConfigEntry(it.key, it.value), AlterConfigOp.OpType.SET) }
            }

        incrementalAlterConfigs(alterConfig).all().get(30, TimeUnit.SECONDS)
    }
}