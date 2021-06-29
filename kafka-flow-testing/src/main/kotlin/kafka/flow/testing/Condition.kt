package kafka.flow.testing

public class Condition<Record>(private val description: String, private val assertionBlock: (Record) -> Any) {
    public fun test(record: Record) {
        val result = assertionBlock.invoke(record)
        if (result is Boolean) throw IllegalArgumentException("The condition block should be an assertion not a predicate (crash if failed, but does return nothing when pass)")
    }

    public fun description(): String = "    $description"
    public fun descriptionFor(record: Record): String {
        val result = runCatching { assertionBlock.invoke(record) }
        return when (result.isSuccess) {
            true -> "    OK   -> $description"
            false -> "    FAIL -> $description\n${result.exceptionOrNull()?.message}"
        }
    }
}