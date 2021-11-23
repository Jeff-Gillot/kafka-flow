package kafka.flow.utils

import be.delta.flow.time.milliseconds
import be.delta.flow.time.seconds
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public class FlowDebouncer<Input, Key> {

    private val mutex = Mutex()
    private val keyTime = HashMap<Key, Instant>()
    private val values = HashMap<Key, Input>()
    private var outputLoop: Job? = null
    private var cleanupLoop: Job? = null

    public suspend fun start(
        flow: Flow<Input>,
        keyProvider: (Input) -> Key?,
        timeProvider: (Input, Instant?) -> Instant?,
        maxDebounceTime: Duration,
        interval: Duration,
        cleanUpInterval: Duration,
    ): Flow<Action<Input>> {
        val outputChannel = Channel<Action<Input>>()

        startInputLoop(flow, keyProvider, timeProvider, outputChannel)
        startOutputLoop(interval, outputChannel)
        startCleanupLoop(cleanUpInterval, maxDebounceTime)

        return outputChannel.receiveAsFlow()
    }

    private suspend fun startOutputLoop(
        interval: Duration,
        outputChannel: Channel<Action<Input>>
    ) {
        outputLoop = CoroutineScope(currentCoroutineContext()).launch {
            val durationInMs = interval.toMillis()
            while (isActive) {
                delay(durationInMs)
                getItemToSend().forEach { outputChannel.send(Process(it)) }
            }
        }
    }

    private suspend fun startCleanupLoop(
        cleanUpInterval: Duration,
        maxDebounceTime: Duration
    ) {
        cleanupLoop = CoroutineScope(currentCoroutineContext()).launch {
            val durationInMs = cleanUpInterval.toMillis()
            while (isActive) {
                delay(durationInMs)
                cleanOldData(maxDebounceTime)
            }
        }
    }

    private suspend fun getItemToSend(): List<Input> = mutex.withLock {
        keyTime
            .filter { (_, value) -> value < Instant.now() }
            .keys
            .mapNotNull { key -> values.remove(key) }

    }

    private suspend fun cleanOldData(maxDebounceTime: Duration) {
        mutex.withLock {
            keyTime
                .filter { (_, value) -> value + maxDebounceTime < Instant.now() }
                .keys
                .forEach { key ->
                    keyTime.remove(key)
                    values.remove(key)
                }
        }
    }

    private suspend fun startInputLoop(
        flow: Flow<Input>,
        keyProvider: (Input) -> Key?,
        timeProvider: (Input, Instant?) -> Instant?,
        outputChannel: Channel<Action<Input>>
    ) {
        val coroutineContext = currentCoroutineContext()
        CoroutineScope(currentCoroutineContext()).launch {
            flow.collect { value: Input ->
                val actions: List<Action<Input>> = mutex.withLock {
                    val key = keyProvider.invoke(value)
                    var oldValue: Input? = null
                    var time: Instant? = null
                    if (key != null) {
                        oldValue = values[key]
                        val oldTime = keyTime[key]
                        time = timeProvider.invoke(value, oldTime)
                        if (time != null) {
                            keyTime[key] = time
                            values[key] = value
                        } else {
                            keyTime[key] = Instant.now()
                        }
                    }
                    val skipAction: Action<Input>? = oldValue?.let { Skip(it) }
                    val processAction: Action<Input>? = if (time == null) Process(value) else null
                    listOfNotNull(skipAction, processAction)
                }
                actions.forEach { action ->
                    outputChannel.send(action)
                }
            }
        }.invokeOnCompletion {
            outputLoop?.cancel()
            cleanupLoop?.cancel()
            CoroutineScope(coroutineContext).launch {
                launch {
                    try {
                        delay(10000)
                        currentCoroutineContext().cancelChildren()
                    } finally {
                        outputChannel.close()
                    }
                }
                launch {
                    values.values.forEach {
                        outputChannel.send(Process(it))
                    }
                }
            }
        }
    }

    public interface Action<Input> {
        public val data: Input
    }

    public data class Process<Input>(public override val data: Input) : Action<Input>
    public data class Skip<Input>(public override val data: Input) : Action<Input>

    public companion object {
        public suspend fun <Input, Key> Flow<Input>.debounce(
            keyProvider: (Input) -> Key,
            timeProvider: (Input, Instant?) -> Instant?,
            maxDebounceTime: Duration,
            interval: Duration = 10.milliseconds(),
            cleanUpInterval: Duration = 10.seconds(),
        ): Flow<Action<Input>> = FlowDebouncer<Input, Key>().start(
            this,
            keyProvider,
            timeProvider,
            maxDebounceTime,
            interval,
            cleanUpInterval
        )

        public suspend fun <Input, Key> Flow<Input>.debounceAndIgnoreSkip(
            keyProvider: (Input) -> Key,
            timeProvider: (Input, Instant?) -> Instant?,
            maxDebounceTime: Duration,
            interval: Duration = 10.milliseconds(),
            cleanUpInterval: Duration = 10.seconds(),
        ): Flow<Input> = debounce(keyProvider, timeProvider, maxDebounceTime, interval, cleanUpInterval)
            .ignoreSkipActions()

        public fun <Input> Flow<Action<Input>>.ignoreSkipActions(): Flow<Input> = mapNotNull { if (it is Process<Input>) it.data else null }
    }
}