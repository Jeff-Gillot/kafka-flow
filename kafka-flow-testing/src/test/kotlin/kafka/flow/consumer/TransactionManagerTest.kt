package kafka.flow.consumer

import kafka.flow.consumer.with.group.id.TransactionManager
import kafka.flow.consumer.with.group.id.WithTransaction
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
        val transactionTopic1Partition1Offset1 = WithTransaction(topic1Partition1, 1L, transactionManager)

        transactionTopic1Partition1Offset1.register()
        transactionTopic1Partition1Offset1.unlock()

        val result = transactionManager.computeOffsetsToCommit(assignment)

        expectThat(result).hasSize(1)
        expectThat(result).containsKey(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(2L)
    }

    @Test
    fun offsetsToCommitNotAllCommitted() = runTest {
        val transactionManager = TransactionManager(1000)
        val transactionTopic1Partition1Offset1 = WithTransaction(topic1Partition1, 1L, transactionManager)
        val transactionTopic1Partition1Offset2 = WithTransaction(topic1Partition1, 2L, transactionManager)
        val transactionTopic1Partition1Offset3 = WithTransaction(topic1Partition1, 3L, transactionManager)
        val transactionTopic1Partition1Offset4 = WithTransaction(topic1Partition1, 4L, transactionManager)

        transactionTopic1Partition1Offset1.register()
        transactionTopic1Partition1Offset2.register()
        transactionTopic1Partition1Offset3.register()
        transactionTopic1Partition1Offset4.register()
        transactionTopic1Partition1Offset1.unlock()
        transactionTopic1Partition1Offset2.unlock()

        val result = transactionManager.computeOffsetsToCommit(assignment)

        expectThat(result).hasSize(1)
        expectThat(result).containsKey(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(3L)
    }

    @Test
    fun cleanCommittedOffset() = runTest {
        val transactionManager = TransactionManager(1000)
        val transactionTopic1Partition1Offset1 = WithTransaction(topic1Partition1, 1L, transactionManager)

        transactionTopic1Partition1Offset1.register()
        transactionTopic1Partition1Offset1.unlock()

        transactionManager.removeCommittedOffsets(transactionManager.computeOffsetsToCommit(assignment))

        val result = transactionManager.computeOffsetsToCommit(assignment)

        expectThat(result).isEmpty()
    }

    @Test
    fun offsetsToCommitMultiStep() = runTest {
        val transactionManager = TransactionManager(1000)
        val transactionTopic1Partition1Offset1 = WithTransaction(topic1Partition1, 1L, transactionManager)
        val transactionTopic1Partition1Offset2 = WithTransaction(topic1Partition1, 2L, transactionManager)
        val transactionTopic1Partition1Offset3 = WithTransaction(topic1Partition1, 3L, transactionManager)
        val transactionTopic1Partition1Offset4 = WithTransaction(topic1Partition1, 4L, transactionManager)

        transactionTopic1Partition1Offset1.register()
        transactionTopic1Partition1Offset2.register()
        transactionTopic1Partition1Offset3.register()
        transactionTopic1Partition1Offset4.register()
        transactionTopic1Partition1Offset1.unlock()
        transactionTopic1Partition1Offset2.unlock()

        transactionManager.removeCommittedOffsets(transactionManager.computeOffsetsToCommit(assignment))

        transactionTopic1Partition1Offset3.unlock()
        transactionTopic1Partition1Offset4.unlock()

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(1)
        expectThat(result).containsKey(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(5L)
    }

    @Test
    fun offsetsToCommitOutOfOrderNothingToCommit() = runTest {
        val transactionManager = TransactionManager(1000)
        val transactionTopic1Partition1Offset1 = WithTransaction(topic1Partition1, 1L, transactionManager)
        val transactionTopic1Partition1Offset2 = WithTransaction(topic1Partition1, 2L, transactionManager)
        val transactionTopic1Partition1Offset3 = WithTransaction(topic1Partition1, 3L, transactionManager)
        val transactionTopic1Partition1Offset4 = WithTransaction(topic1Partition1, 4L, transactionManager)

        transactionTopic1Partition1Offset1.register()
        transactionTopic1Partition1Offset2.register()
        transactionTopic1Partition1Offset3.register()
        transactionTopic1Partition1Offset4.register()

        transactionTopic1Partition1Offset3.unlock()
        transactionTopic1Partition1Offset4.unlock()

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(1)
        expectThat(result).containsKey(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(1L)
    }


    @Test
    fun offsetsToCommitOutOfOrderPartialToCommit() = runTest {
        val transactionManager = TransactionManager(1000)
        val transactionTopic1Partition1Offset1 = WithTransaction(topic1Partition1, 1L, transactionManager)
        val transactionTopic1Partition1Offset2 = WithTransaction(topic1Partition1, 2L, transactionManager)
        val transactionTopic1Partition1Offset3 = WithTransaction(topic1Partition1, 3L, transactionManager)
        val transactionTopic1Partition1Offset4 = WithTransaction(topic1Partition1, 4L, transactionManager)

        transactionTopic1Partition1Offset1.register()
        transactionTopic1Partition1Offset2.register()
        transactionTopic1Partition1Offset3.register()
        transactionTopic1Partition1Offset4.register()

        transactionTopic1Partition1Offset3.unlock()
        transactionTopic1Partition1Offset4.unlock()
        transactionTopic1Partition1Offset1.unlock()

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(1)
        expectThat(result).containsKey(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(2L)
    }

    @Test
    fun offsetsToCommitMultipleTopicsAndPartition() = runTest {
        val transactionManager = TransactionManager(1000)
        val transactionTopic1Partition1Offset1 = WithTransaction(topic1Partition1, 1L, transactionManager)
        val transactionTopic1Partition1Offset2 = WithTransaction(topic1Partition1, 2L, transactionManager)
        val transactionTopic1Partition1Offset3 = WithTransaction(topic1Partition1, 3L, transactionManager)
        val transactionTopic1Partition1Offset4 = WithTransaction(topic1Partition1, 4L, transactionManager)
        val transactionTopic1Partition2Offset1 = WithTransaction(topic1Partition2, 1L, transactionManager)
        val transactionTopic1Partition2Offset2 = WithTransaction(topic1Partition2, 2L, transactionManager)
        val transactionTopic1Partition2Offset3 = WithTransaction(topic1Partition2, 3L, transactionManager)
        val transactionTopic1Partition2Offset4 = WithTransaction(topic1Partition2, 4L, transactionManager)

        transactionTopic1Partition1Offset1.register()
        transactionTopic1Partition1Offset2.register()
        transactionTopic1Partition1Offset3.register()
        transactionTopic1Partition1Offset4.register()
        transactionTopic1Partition2Offset1.register()
        transactionTopic1Partition2Offset2.register()
        transactionTopic1Partition2Offset3.register()
        transactionTopic1Partition2Offset4.register()

        transactionTopic1Partition1Offset1.unlock()
        transactionTopic1Partition1Offset2.unlock()
        transactionTopic1Partition2Offset4.unlock()
        transactionTopic1Partition2Offset1.unlock()

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(2)
        expectThat(result).containsKeys(topic1Partition1, topic1Partition2)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(3L)
        expectThat(result[topic1Partition2]!!.offset()).isEqualTo(2L)
    }

    @Test
    fun offsetsToCommitMultipleTopicsAndPartitionOnlyCommitOnce() = runTest {
        val transactionManager = TransactionManager(1000)
        val transactionTopic1Partition1Offset1 = WithTransaction(topic1Partition1, 1L, transactionManager)
        val transactionTopic1Partition1Offset2 = WithTransaction(topic1Partition1, 2L, transactionManager)
        val transactionTopic1Partition1Offset3 = WithTransaction(topic1Partition1, 3L, transactionManager)
        val transactionTopic1Partition1Offset4 = WithTransaction(topic1Partition1, 4L, transactionManager)
        val transactionTopic2Partition1Offset1 = WithTransaction(topic2Partition1, 1L, transactionManager)
        val transactionTopic2Partition1Offset2 = WithTransaction(topic2Partition1, 2L, transactionManager)
        val transactionTopic2Partition1Offset3 = WithTransaction(topic2Partition1, 3L, transactionManager)
        val transactionTopic2Partition1Offset4 = WithTransaction(topic2Partition1, 4L, transactionManager)

        transactionTopic1Partition1Offset1.register()
        transactionTopic1Partition1Offset2.register()
        transactionTopic1Partition1Offset3.register()
        transactionTopic1Partition1Offset4.register()
        transactionTopic2Partition1Offset1.register()
        transactionTopic2Partition1Offset2.register()
        transactionTopic2Partition1Offset3.register()
        transactionTopic2Partition1Offset4.register()

        transactionTopic1Partition1Offset1.unlock()
        transactionTopic1Partition1Offset2.unlock()
        transactionTopic2Partition1Offset4.unlock()
        transactionManager.removeCommittedOffsets(transactionManager.computeOffsetsToCommit(assignment))

        transactionTopic2Partition1Offset1.unlock()

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(2)
        expectThat(result).containsKeys(topic1Partition1, topic2Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(3L)
        expectThat(result[topic2Partition1]!!.offset()).isEqualTo(2L)
    }

    @Test
    fun increaseCountNoCommit() = runTest {
        val transactionManager = TransactionManager(1000)
        val transactionTopic1Partition1Offset1 = WithTransaction(topic1Partition1, 1L, transactionManager)

        transactionTopic1Partition1Offset1.register()

        repeat(10) {
            transactionTopic1Partition1Offset1.lock()
        }

        transactionTopic1Partition1Offset1.unlock()

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(1)
        expectThat(result).containsKey(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(1L)
    }

    @Test
    fun increaseCountCommit() = runTest {
        val transactionManager = TransactionManager(1000)
        val transactionTopic1Partition1Offset1 = WithTransaction(topic1Partition1, 1L, transactionManager)

        transactionTopic1Partition1Offset1.register()

        repeat(10) {
            transactionTopic1Partition1Offset1.lock()
        }

        repeat(11) {
            transactionTopic1Partition1Offset1.unlock()
        }

        val result = transactionManager.computeOffsetsToCommit(assignment)
        expectThat(result).hasSize(1)
        expectThat(result).containsKeys(topic1Partition1)
        expectThat(result[topic1Partition1]!!.offset()).isEqualTo(2L)
    }
}