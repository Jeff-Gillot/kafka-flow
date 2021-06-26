package kafka.flow.consumer

import kafka.flow.consumer.with.group.id.KafkaFlowConsumerWithGroupId
import kafka.flow.consumer.with.group.id.Transaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

public sealed interface KafkaMessage<Key, PartitionKey, Value, Output>

public data class Record<Key, PartitionKey, Value, Output>(
    val consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
    val key: Key,
    val partitionKey: PartitionKey,
    val value: Value,
    val output: Output,
    val transaction: Transaction?
) : KafkaMessage<Key, PartitionKey, Value, Output>

public interface PartitionChangedMessage<Key, PartitionKey, Value, Output> : KafkaMessage<Key, PartitionKey, Value, Output>{
    public val newAssignment: List<TopicPartition>
}
public data class PartitionsAssigned<Key, PartitionKey, Value, Output>(
    val newlyAssignedPartitions: List<TopicPartition>,
    override val newAssignment: List<TopicPartition>
) : KafkaMessage<Key, PartitionKey, Value, Output>, PartitionChangedMessage<Key, PartitionKey, Value, Output>

public data class PartitionsRevoked<Key, PartitionKey, Value, Output>(
    val revokedPartitions: List<TopicPartition>,
    override val newAssignment: List<TopicPartition>
) : KafkaMessage<Key, PartitionKey, Value, Output>, PartitionChangedMessage<Key, PartitionKey, Value, Output>

public interface FlowControlMessage<Key, PartitionKey, Value, Output> : KafkaMessage<Key, PartitionKey, Value, Output>
public data class StartConsuming<Key, PartitionKey, Value, Output>(public val client: KafkaFlowConsumerWithGroupId) : KafkaMessage<Key, PartitionKey, Value, Output>, FlowControlMessage<Key, PartitionKey, Value, Output>
public class StopConsuming<Key, PartitionKey, Value, Output> : KafkaMessage<Key, PartitionKey, Value, Output>, FlowControlMessage<Key, PartitionKey, Value, Output>
public class EndOfBatch<Key, PartitionKey, Value, Output> : KafkaMessage<Key, PartitionKey, Value, Output>, FlowControlMessage<Key, PartitionKey, Value, Output>

