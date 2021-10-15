@file:Suppress("UNCHECKED_CAST")

package kafka.flow.consumer

import be.delta.flow.time.seconds
import kafka.flow.TopicDescriptor
import kafka.flow.consumer.processor.*
import kafka.flow.consumer.with.group.id.MaybeTransaction
import kafka.flow.consumer.with.group.id.WithTransaction
import kafka.flow.consumer.with.group.id.WithoutTransaction
import kafka.flow.producer.KafkaOutput
import kotlinx.coroutines.flow.*
import org.apache.kafka.common.TopicPartition
import java.time.Duration

public suspend fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output, WithoutTransaction>>.collectValues(block: suspend (Value) -> Unit) {
    this.collect {
        if (it is Record) block.invoke(it.value)
    }
}

public suspend fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output, WithTransaction>>.collectValues(block: suspend (Value, WithTransaction) -> Unit) {
    this.collect {
        if (it is Record) block.invoke(it.value, it.transaction)
    }
}


public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.collectRecords(
    block: suspend (Record<Key, Partition, Value, Output, Transaction>) -> Unit
) {
    this.collect {
        if (it is Record) block.invoke(it)
    }
}

public suspend fun <KeyIn, PartitionIn, ValueIn, OutputIn, TransactionIn : MaybeTransaction, KeyOut, PartitionOut, ValueOut, OutputOut, TransactionOut : MaybeTransaction> Flow<KafkaMessage<KeyIn, PartitionIn, ValueIn, OutputIn, TransactionIn>>.mapRecord(
    block: suspend (Record<KeyIn, PartitionIn, ValueIn, OutputIn, TransactionIn>) -> Record<KeyOut, PartitionOut, ValueOut, OutputOut, TransactionOut>
): Flow<KafkaMessage<KeyOut, PartitionOut, ValueOut, OutputOut, TransactionOut>> {
    return map { kafkaMessage ->
        if (kafkaMessage is Record) {
            block.invoke(kafkaMessage)
        } else {
            kafkaMessage as KafkaMessage<KeyOut, PartitionOut, ValueOut, OutputOut, TransactionOut>
        }
    }
}

public suspend fun <KeyIn, PartitionIn, ValueIn, OutputIn, TransactionIn : MaybeTransaction, KeyOut, PartitionOut, ValueOut, OutputOut, TransactionOut : MaybeTransaction> Flow<KafkaMessage<KeyIn, PartitionIn, ValueIn, OutputIn, TransactionIn>>.mapRecordNotNull(
    block: suspend (Record<KeyIn, PartitionIn, ValueIn, OutputIn, TransactionIn>) -> Record<KeyOut, PartitionOut, ValueOut, OutputOut, TransactionOut>?
): Flow<KafkaMessage<KeyOut, PartitionOut, ValueOut, OutputOut, TransactionOut>> {
    return mapNotNull { kafkaMessage ->
        if (kafkaMessage is Record) {
            block.invoke(kafkaMessage)
        } else {
            kafkaMessage as KafkaMessage<KeyOut, PartitionOut, ValueOut, OutputOut, TransactionOut>
        }
    }
}

public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Unit, Output, Transaction>>.deserializeValue(block: suspend (ByteArray) -> Value): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return mapRecord { record ->
        Record(
            record.consumerRecord,
            record.key,
            record.partitionKey,
            block.invoke(record.consumerRecord.value()),
            record.timestamp,
            record.output,
            record.transaction
        )
    }
}

public fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output, WithoutTransaction>>.values(): Flow<Value> {
    return filterIsInstance<Record<Key, Partition, Value, Output, WithoutTransaction>>()
        .map { it.value }
}

@JvmName("valuesKeyPartitionValueOutputWithTransaction")
public fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output, WithTransaction>>.values(): Flow<Pair<Key, Value>> {
    return filterIsInstance<Record<Key, Partition, Value, Output, WithTransaction>>()
        .map { Pair(it.key, it.value) }
}

public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.onEachRecord(
    block: suspend (Record<Key, Partition, Value, Output, Transaction>) -> Unit
): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return onEach { message ->
        if (message is Record)
            block.invoke(message)
    }
}

public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.onStartConsuming(
    block: suspend (client: KafkaFlowConsumer<Flow<KafkaMessage<Unit, Unit, Unit, Unit, WithoutTransaction>>>) -> Unit
): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return onEach { message ->
        if (message is StartConsuming)
            block.invoke(message.client)
    }
}

public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.onStopConsuming(
    block: suspend () -> Unit
): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return onEach { message ->
        if (message is StopConsuming)
            block.invoke()
    }
}

public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.onEndOfBatch(
    block: suspend () -> Unit
): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return onEach { message ->
        if (message is EndOfBatch)
            block.invoke()
    }
}

public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.onPartitionAssigned(
    block: suspend (newlyAssignedPartition: List<TopicPartition>, newAssignment: List<TopicPartition>) -> Unit
): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return onEach { message ->
        if (message is PartitionsAssigned)
            block.invoke(message.newlyAssignedPartitions, message.newAssignment)
    }
}

public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.onPartitionRevoked(
    block: suspend (revokedPartition: List<TopicPartition>, newAssignment: List<TopicPartition>) -> Unit
): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return onEach { message ->
        if (message is PartitionsRevoked)
            block.invoke(message.revokedPartitions, message.newAssignment)
    }
}


public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.onPartitionChanged(
    block: suspend (newAssignment: List<TopicPartition>) -> Unit
): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return onEach { message ->
        if (message is PartitionChangedMessage)
            block.invoke(message.newAssignment)
    }
}

public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Unit, Unit, Unit, Output, Transaction>>.deserializeUsing(
    topicDescriptor: TopicDescriptor<Key, Partition, Value>,
    onDeserializationException: suspend (Throwable) -> Unit = { it.printStackTrace() }
): Flow<KafkaMessage<Key, Partition, Value?, Output, Transaction>> {
    return deserializeUsing(listOf(topicDescriptor), onDeserializationException)
}

public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Unit, Unit, Unit, Output, Transaction>>.deserializeUsing(
    topicDescriptors: List<TopicDescriptor<out Key, out Partition, out Value>>,
    onDeserializationException: suspend (Throwable) -> Unit = { it.printStackTrace() }
): Flow<KafkaMessage<Key, Partition, Value?, Output, Transaction>> {
    return transform(TopicDescriptorDeserializerProcessor(topicDescriptors as List<TopicDescriptor<Key, Partition, Value>>, onDeserializationException))
}

public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value?, Output, Transaction>>.ignoreTombstones(): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return filter { message ->
        if (message is Record) {
            val result = message.value != null
            if(!result) message.transaction.unlock()
            result
        } else {
            true
        }
    } as Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>
}

public suspend fun <KeyIn, PartitionIn, ValueIn, OutputIn, TransactionIn : MaybeTransaction, KeyOut, PartitionOut, ValueOut, OutputOut, TransactionOut : MaybeTransaction> Flow<KafkaMessage<KeyIn, PartitionIn, ValueIn, OutputIn, TransactionIn>>.transform(
    processor: TransformProcessor<KeyIn, PartitionIn, ValueIn, OutputIn, TransactionIn, KeyOut, PartitionOut, ValueOut, OutputOut, TransactionOut>
): Flow<KafkaMessage<KeyOut, PartitionOut, ValueOut, OutputOut, TransactionOut>> {
    return this
        .onStartConsuming(processor::startConsuming)
        .onStopConsuming(processor::stopConsuming)
        .onCompletion { processor.completion() }
        .onEndOfBatch(processor::endOfBatch)
        .onPartitionAssigned(processor::partitionAssigned)
        .onPartitionRevoked(processor::partitionRevoked)
        .mapRecordNotNull { processor.record(it.consumerRecord, it.key, it.partitionKey, it.value, it.timestamp, it.output, it.transaction) }
}

public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.collect(
    processor: Sink<Key, Partition, Value, Output, Transaction>
) {
    return this
        .onStartConsuming(processor::startConsuming)
        .onStopConsuming(processor::stopConsuming)
        .onCompletion { processor.completion() }
        .onEndOfBatch(processor::endOfBatch)
        .onPartitionAssigned(processor::partitionAssigned)
        .onPartitionRevoked(processor::partitionRevoked)
        .onEachRecord { processor.record(it.consumerRecord, it.key, it.partitionKey, it.value, it.timestamp, it.output, it.transaction) }
        .collect()
}

public suspend fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output, WithoutTransaction>>.createTransactions(
    maxOpenTransactions: Int = 1024,
    commitInterval: Duration = 30.seconds()
): Flow<KafkaMessage<Key, Partition, Value, Output, WithTransaction>> {
    return this.transform(TransactionProcessor(maxOpenTransactions, commitInterval))
}

public suspend fun <Key, Partition, Value> Flow<KafkaMessage<Key, Partition, Value, KafkaOutput, WithTransaction>>.writeOutputToKafkaAndCommit() {
    this.collect(KafkaWriterSink())
}

@JvmName("writeOutputToKafkaAndCommitKeyPartitionValueUnitWithTransactionKafkaOutput")
public suspend fun <Key, Partition, Value> Flow<Pair<List<KafkaMessage<Key, Partition, Value, Unit, WithTransaction>>, KafkaOutput>>.writeOutputToKafkaAndCommit() {
    val kafkaWriter = BufferedKafkaWriterSink<Key, Partition, Value, WithTransaction>()
    this.collect { kafkaWriter.handleRecords(it) }
}

public suspend fun <Key, Partition, Value> Flow<KafkaMessage<Key, Partition, Value, KafkaOutput, WithoutTransaction>>.writeOutputToKafka() {
    this.collect(KafkaWriterSink())
}

public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Unit, Transaction>>.mapValueToOutput(
    block: suspend (Key, Value) -> Output
): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return mapRecord { record -> Record(record.consumerRecord, record.key, record.partitionKey, record.value, record.timestamp, block.invoke(record.key, record.value), record.transaction) }
}

public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.groupByPartitionKey(
    processorTimeout: Duration,
    channelCapacity: Int = 10,
    flowFactory: suspend (Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>, partitionKey: Partition) -> Unit
) {
    return collect(GroupingProcessor(processorTimeout, channelCapacity, flowFactory))
}

public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.filterValue(
    predicate: suspend (Value) -> Boolean
): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return filter { message ->
        if (message is Record) {
            val result = predicate.invoke(message.value)
            if (!result) message.transaction.unlock()
            result
        } else {
            true
        }
    }
}

public fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>>.onEachTombstone(
    block: suspend (Record<Key, Partition, Value, Output, Transaction>) -> Unit
): Flow<KafkaMessage<Key, Partition, Value, Output, Transaction>> {
    return onEach { message ->
        if (message is Record && message.value == null)
            block.invoke(message)
    }
}


public suspend fun <Key, Partition, Value, Transaction : MaybeTransaction> Flow<KafkaMessage<Key, Partition, Value, Unit, Transaction>>.batchRecords(
    batchSize: Int, timeSpan: Duration
): Flow<List<KafkaMessage<Key, Partition, Value, Unit, Transaction>>> {
    return BufferProcessor<Key, Partition, Value, Transaction>(batchSize, timeSpan).start(this)
}


public suspend fun <Key, Partition, Value, Output, Transaction : MaybeTransaction> Flow<List<KafkaMessage<Key, Partition, Value, Unit, Transaction>>>.mapValuesToOutput(
    block: suspend (List<Pair<Key, Value>>) -> Output
): Flow<Pair<List<KafkaMessage<Key, Partition, Value, Unit, Transaction>>, Output>> {
    return map { records ->
        val keyValues = records
            .filterIsInstance<Record<Key, Partition, Value, Unit, Transaction>>()
            .map { it.key to it.value }
        records to block.invoke(keyValues)
    }
}

