package kafka.flow.consumer

import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.consumer.with.group.id.WithoutTransaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

public sealed interface KafkaMessage<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>

public data class Record<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>(
    val consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
    val key: Key,
    val partitionKey: PartitionKey,
    val value: Value,
    val output: Output,
    val transaction: Transaction
) : KafkaMessage<Key, PartitionKey, Value, Output, Transaction>

public interface PartitionChangedMessage<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction> : KafkaMessage<Key, PartitionKey, Value, Output, Transaction> {
    public val newAssignment: List<TopicPartition>
}

public data class PartitionsAssigned<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>(
    val newlyAssignedPartitions: List<TopicPartition>,
    override val newAssignment: List<TopicPartition>
) : PartitionChangedMessage<Key, PartitionKey, Value, Output, Transaction>

public data class PartitionsRevoked<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>(
    val revokedPartitions: List<TopicPartition>,
    override val newAssignment: List<TopicPartition>
) : PartitionChangedMessage<Key, PartitionKey, Value, Output, Transaction>

public interface FlowControlMessage<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction> : KafkaMessage<Key, PartitionKey, Value, Output, Transaction>
public data class StartConsuming<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>(
    public val client: KafkaFlowConsumer<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>
) : FlowControlMessage<Key, PartitionKey, Value, Output, Transaction>

public class StopConsuming<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction> : FlowControlMessage<Key, PartitionKey, Value, Output, Transaction>
public class EndOfBatch<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction> : FlowControlMessage<Key, PartitionKey, Value, Output, Transaction>




