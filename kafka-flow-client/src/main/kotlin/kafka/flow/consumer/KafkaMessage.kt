package kafka.flow.consumer

import java.time.Instant
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kotlinx.coroutines.flow.Flow
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

public sealed interface KafkaMessage<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>

public data class Record<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>(
    val consumerRecord: ConsumerRecord<ByteArray, ByteArray>,
    val key: Key,
    val partitionKey: PartitionKey,
    val value: Value,
    val timestamp: Instant,
    val output: Output,
    val transaction: Transaction
) : KafkaMessage<Key, PartitionKey, Value, Output, Transaction> {
    public fun <NewOutput> withOutput(newOutput: NewOutput): KafkaMessage<Key, PartitionKey, Value, NewOutput, Transaction> =
        Record(consumerRecord, key, partitionKey, value, timestamp, newOutput, transaction)

    public fun <NewValue> withValue(newValue: NewValue): KafkaMessage<Key, PartitionKey, NewValue, Output, Transaction> =
        Record(consumerRecord, key, partitionKey, newValue, timestamp, output, transaction)

    public fun <NewTransaction : MaybeTransaction> withTransaction(newTransaction: NewTransaction): KafkaMessage<Key, PartitionKey, Value, Output, NewTransaction> =
        Record(consumerRecord, key, partitionKey, value, timestamp, output, newTransaction)
}

public interface FlowControlMessage<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction> : KafkaMessage<Key, PartitionKey, Value, Output, Transaction> {
    @Suppress("UNCHECKED_CAST")
    public fun <NewOutput> withOutputType(): KafkaMessage<Key, PartitionKey, Value, NewOutput, Transaction> =
        this as KafkaMessage<Key, PartitionKey, Value, NewOutput, Transaction>

    @Suppress("UNCHECKED_CAST")
    public fun <NewValue> withValueType(): KafkaMessage<Key, PartitionKey, NewValue, Output, Transaction> =
        this as KafkaMessage<Key, PartitionKey, NewValue, Output, Transaction>

    @Suppress("UNCHECKED_CAST")
    public fun <NewTransaction : MaybeTransaction> withTransactionType(): KafkaMessage<Key, PartitionKey, Value, Output, NewTransaction> =
        this as KafkaMessage<Key, PartitionKey, Value, Output, NewTransaction>
}

public interface PartitionChangedMessage<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction> : FlowControlMessage<Key, PartitionKey, Value, Output, Transaction> {
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

public data class StartConsuming<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction>(
    public val client: KafkaFlowConsumer<Flow<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>>
) : FlowControlMessage<Key, PartitionKey, Value, Output, Transaction>

public class StopConsuming<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction> : FlowControlMessage<Key, PartitionKey, Value, Output, Transaction>
public class EndOfBatch<Key, PartitionKey, Value, Output, Transaction : MaybeTransaction> : FlowControlMessage<Key, PartitionKey, Value, Output, Transaction>




