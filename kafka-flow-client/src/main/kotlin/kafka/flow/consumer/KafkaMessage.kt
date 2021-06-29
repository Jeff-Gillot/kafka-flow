package kafka.flow.consumer

import kafka.flow.consumer.with.group.id.KafkaFlowConsumerWithGroupIdImpl
import kafka.flow.consumer.with.group.id.Transaction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

public sealed interface KafkaMessage<Key, PartitionKey, Value, Output>
public sealed interface KafkaMessageWithTransaction<Key, PartitionKey, Value, Output>

public data class RecordWithTransaction<Key, PartitionKey, Value, Output>(
    val consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
    val key: Key,
    val partitionKey: PartitionKey,
    val value: Value,
    val output: Output,
    val transaction: Transaction
) : KafkaMessageWithTransaction<Key, PartitionKey, Value, Output> {
    public constructor(record: Record<Key, PartitionKey, Value, Output>, transaction: Transaction) : this(record.consumerRecord, record.key, record.partitionKey, record.value, record.output, transaction)
}

public data class Record<Key, PartitionKey, Value, Output>(
    val consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
    val key: Key,
    val partitionKey: PartitionKey,
    val value: Value,
    val output: Output,
) : KafkaMessage<Key, PartitionKey, Value, Output>

public interface PartitionChangedMessage<Key, PartitionKey, Value, Output> : KafkaMessage<Key, PartitionKey, Value, Output>, KafkaMessageWithTransaction<Key, PartitionKey, Value, Output> {
    public val newAssignment: List<TopicPartition>
}

public data class PartitionsAssigned<Key, PartitionKey, Value, Output>(
    val newlyAssignedPartitions: List<TopicPartition>,
    override val newAssignment: List<TopicPartition>
) : PartitionChangedMessage<Key, PartitionKey, Value, Output>

public data class PartitionsRevoked<Key, PartitionKey, Value, Output>(
    val revokedPartitions: List<TopicPartition>,
    override val newAssignment: List<TopicPartition>
) : PartitionChangedMessage<Key, PartitionKey, Value, Output>

public interface FlowControlMessage<Key, PartitionKey, Value, Output> : KafkaMessage<Key, PartitionKey, Value, Output>, KafkaMessageWithTransaction<Key, PartitionKey, Value, Output>
public data class StartConsuming<Key, PartitionKey, Value, Output>(public val client: KafkaFlowConsumer<KafkaMessage<Unit, Unit, Unit, Unit>>) : FlowControlMessage<Key, PartitionKey, Value, Output>
public class StopConsuming<Key, PartitionKey, Value, Output> : FlowControlMessage<Key, PartitionKey, Value, Output>
public class EndOfBatch<Key, PartitionKey, Value, Output> : FlowControlMessage<Key, PartitionKey, Value, Output>

