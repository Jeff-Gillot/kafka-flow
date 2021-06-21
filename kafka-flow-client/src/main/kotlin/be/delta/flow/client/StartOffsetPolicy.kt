package be.delta.flow.client

import java.time.Duration
import java.time.Instant

public sealed interface StartOffsetPolicy {
    public companion object {
        public fun earliest(): Earliest = Earliest
        public fun latest(): Latest = Latest
        public fun specificTime(offsetTime: Instant): SpecificTime = SpecificTime(offsetTime)
        public fun specificOffsetFromNow(duration: Duration): SpecificOffsetFromNow = SpecificOffsetFromNow(duration)
    }

    public object Earliest: StartOffsetPolicy
    public object Latest: StartOffsetPolicy
    public class SpecificTime(public val offsetTime: Instant): StartOffsetPolicy
    public class SpecificOffsetFromNow(public val duration: Duration): StartOffsetPolicy
}


