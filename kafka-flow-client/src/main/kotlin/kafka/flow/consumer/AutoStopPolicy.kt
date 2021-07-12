package kafka.flow.consumer

import java.time.Duration
import java.time.Instant

public sealed interface AutoStopPolicy {
    public companion object {
        public fun never(): Never = Never
        public fun whenUpToDate(): WhenUpToDate = WhenUpToDate
        public fun atSpecificTime(stopTime: Instant): AtSpecificTime = AtSpecificTime(stopTime)
        public fun specificOffsetFromNow(duration: Duration): SpecificOffsetFromNow = SpecificOffsetFromNow(duration)
    }

    public object Never : AutoStopPolicy
    public object WhenUpToDate : AutoStopPolicy
    public data class AtSpecificTime(public val stopTime: Instant) : AutoStopPolicy
    public data class SpecificOffsetFromNow(public val duration: Duration): AutoStopPolicy
}



