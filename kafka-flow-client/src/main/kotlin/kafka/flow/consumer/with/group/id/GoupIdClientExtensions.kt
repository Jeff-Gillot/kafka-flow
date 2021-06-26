package kafka.flow.consumer.with.group.id

import kafka.flow.consumer.KafkaMessage
import kafka.flow.consumer.mapRecord
import kafka.flow.consumer.onStartConsuming
import kafka.flow.consumer.onStopConsuming
import kafka.flow.utils.seconds
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import org.apache.kafka.common.TopicPartition
import java.time.Duration

public suspend fun <Key, Partition, Value, Output> Flow<KafkaMessage<Key, Partition, Value, Output>>.createTransactions(maxOpenTransactions: Int = 1024, commitInterval: Duration = 30.seconds()): Flow<KafkaMessage<Key, Partition, Value, Output>> {
    val transactionManager = TransactionManager(maxOpenTransactions)
    var client: KafkaFlowConsumerWithGroupId? = null
    var commitLoop: Job? = null
    return this
        .onCompletion {
            commitLoop?.cancel()
            client?.let { transactionManager.rollbackAndCommit(it) }
        }
        .onStartConsuming {
            client = it.client
            commitLoop = CoroutineScope(currentCoroutineContext()).launch {
                while (true) {
                    delay(commitInterval.toMillis())
                    transactionManager.rollbackAndCommit(it.client)
                }
            }
        }
        .mapRecord { record ->
            val transaction = Transaction(TopicPartition(record.consumerRecord.topic(), record.consumerRecord.partition()), record.consumerRecord.offset(), transactionManager)
            transaction.lock()
            record.copy(transaction = transaction)
        }
}