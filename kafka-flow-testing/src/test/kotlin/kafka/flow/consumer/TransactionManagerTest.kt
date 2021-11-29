package kafka.flow.consumer

import kafka.flow.consumer.with.group.id.TransactionManager
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import strikt.api.expectThat
import strikt.assertions.containsKey
import strikt.assertions.containsKeys
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo

class TransactionManagerTest {
    private val topic1Partition1 = TopicPartition("topic-1", 1)
    private val topic1Partition2 = TopicPartition("topic-1", 2)
    private val topic2Partition1 = TopicPartition("topic-2", 1)
    private val topic2Partition2 = TopicPartition("topic-2", 2)

    private val assignment = listOf(topic1Partition1, topic1Partition2, topic2Partition1, topic2Partition2)

    @Test
    fun offsetsToCommitAllCommitted() = runTest {
        val transactionManager = TransactionManager(1000)

        transactionManager.registerTransaction(topic1Partition1, 1L)
        transactionManager.decreaseTransaction(topic1Partition1, 1L)

        val result = transactionManager.computeOffsetsToCommit(assignment)

        expectThat(result).hasSize(1)
        expectThat(result).containsKey(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(2L)
    }

    @Test
    fun offsetsToCommitNotAllCommitted() = runTest {
        val transactionManager = TransactionManager(1000)

        transactionManager.registerTransaction(topic1Partition1, 1L)
        transactionManager.registerTransaction(topic1Partition1, 2L)
        transactionManager.registerTransaction(topic1Partition1, 3L)
        transactionManager.registerTransaction(topic1Partition1, 4L)
        transactionManager.decreaseTransaction(topic1Partition1, 1L)
        transactionManager.decreaseTransaction(topic1Partition1, 2L)

        val result = transactionManager.computeOffsetsToCommit(assignment)

        expectThat(result).hasSize(1)
        expectThat(result).containsKey(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(3L)
    }

    @Test
    fun offsetsToCommitCleansInfo() = runTest {
        val transactionManager = TransactionManager(1000)

        transactionManager.registerTransaction(topic1Partition1, 1L)
        transactionManager.decreaseTransaction(topic1Partition1, 1L)

        transactionManager.computeOffsetsToCommit(assignment)
        val result = transactionManager.computeOffsetsToCommit(assignment)

        expectThat(result).isEmpty()
    }

    @Test
    fun offsetsToCommitMultiStep() = runTest {
        val transactionManager = TransactionManager(1000)

        transactionManager.registerTransaction(topic1Partition1, 1L)
        transactionManager.registerTransaction(topic1Partition1, 2L)
        transactionManager.registerTransaction(topic1Partition1, 3L)
        transactionManager.registerTransaction(topic1Partition1, 4L)
        transactionManager.decreaseTransaction(topic1Partition1, 1L)
        transactionManager.decreaseTransaction(topic1Partition1, 2L)

        transactionManager.computeOffsetsToCommit(assignment)

        transactionManager.decreaseTransaction(topic1Partition1, 3L)
        transactionManager.decreaseTransaction(topic1Partition1, 4L)

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(1)
        expectThat(result).containsKey(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(5L)
    }

    @Test
    fun offsetsToCommitOutOfOrderNothingToCommit() = runTest {
        val transactionManager = TransactionManager(1000)

        transactionManager.registerTransaction(topic1Partition1, 1L)
        transactionManager.registerTransaction(topic1Partition1, 2L)
        transactionManager.registerTransaction(topic1Partition1, 3L)
        transactionManager.registerTransaction(topic1Partition1, 4L)
        transactionManager.decreaseTransaction(topic1Partition1, 3L)
        transactionManager.decreaseTransaction(topic1Partition1, 4L)

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).isEmpty()
    }


    @Test
    fun offsetsToCommitOutOfOrderPartialToCommit() = runTest {
        val transactionManager = TransactionManager(1000)

        transactionManager.registerTransaction(topic1Partition1, 1L)
        transactionManager.registerTransaction(topic1Partition1, 2L)
        transactionManager.registerTransaction(topic1Partition1, 3L)
        transactionManager.registerTransaction(topic1Partition1, 4L)
        transactionManager.decreaseTransaction(topic1Partition1, 3L)
        transactionManager.decreaseTransaction(topic1Partition1, 4L)
        transactionManager.decreaseTransaction(topic1Partition1, 1L)

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(1)
        expectThat(result).containsKey(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(2L)
    }

    @Test
    fun offsetsToCommitMultipleTopicsAndPartition() = runTest {
        val transactionManager = TransactionManager(1000)

        listOf(topic1Partition1, topic1Partition2, topic2Partition1, topic2Partition2).forEach { topicPartition ->
            transactionManager.registerTransaction(topicPartition, 1L)
            transactionManager.registerTransaction(topicPartition, 2L)
            transactionManager.registerTransaction(topicPartition, 3L)
            transactionManager.registerTransaction(topicPartition, 4L)
        }

        transactionManager.decreaseTransaction(topic1Partition1, 1L)
        transactionManager.decreaseTransaction(topic1Partition1, 2L)
        transactionManager.decreaseTransaction(topic1Partition2, 4L)
        transactionManager.decreaseTransaction(topic2Partition1, 1L)

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(2)
        expectThat(result).containsKeys(topic1Partition1, topic2Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(3L)
        expectThat(result[topic2Partition1]!!.offset()).isEqualTo(2L)
    }

    @Test
    fun offsetsToCommitMultipleTopicsAndPartitionOnlyCommitOnce() = runTest {
        val transactionManager = TransactionManager(1000)

        listOf(topic1Partition1, topic1Partition2, topic2Partition1, topic2Partition2).forEach { topicPartition ->
            transactionManager.registerTransaction(topicPartition, 1L)
            transactionManager.registerTransaction(topicPartition, 2L)
            transactionManager.registerTransaction(topicPartition, 3L)
            transactionManager.registerTransaction(topicPartition, 4L)
        }

        transactionManager.decreaseTransaction(topic1Partition1, 1L)
        transactionManager.decreaseTransaction(topic1Partition1, 2L)
        transactionManager.decreaseTransaction(topic1Partition2, 4L)
        transactionManager.decreaseTransaction(topic2Partition1, 1L)
        transactionManager.computeOffsetsToCommit(assignment)

        transactionManager.decreaseTransaction(topic2Partition2, 1L)

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(1)
        expectThat(result).containsKeys(topic2Partition2)
        expectThat(result[topic2Partition2]!!.offset()).isEqualTo(2L)
    }

    @Test
    fun increaseCountNoCommit() = runTest {
        val transactionManager = TransactionManager(1000)

        transactionManager.registerTransaction(topic1Partition1, 1L)

        repeat(10) {
            transactionManager.increaseTransaction(topic1Partition1, 1L)
        }

        transactionManager.decreaseTransaction(topic1Partition1, 1L)

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(0)
    }

    @Test
    fun increaseCountCommit() = runTest {
        val transactionManager = TransactionManager(1000)

        transactionManager.registerTransaction(topic1Partition1, 1L)

        repeat(10) {
            transactionManager.increaseTransaction(topic1Partition1, 1L)
        }

        repeat(11) {
            transactionManager.decreaseTransaction(topic1Partition1, 1L)
        }

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(1)
        expectThat(result).containsKeys(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(2L)
    }
}