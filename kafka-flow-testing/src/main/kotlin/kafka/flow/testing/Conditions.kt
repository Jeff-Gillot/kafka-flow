package kafka.flow.testing

public class Conditions<Record>(private val conditions: List<Condition<Record>>) {
    public fun test(record: Record): Result<Unit> {
        return runCatching {
            conditions.forEach { it.test(record) }
        }
    }

    public fun description(): String = conditions.joinToString("\n") { it.description() }
    public fun descriptionFor(record: Record): String {
      return when(test(record).isSuccess) {
          true -> "OK   -> $record\n" + conditions.joinToString("\n") { it.descriptionFor(record) }
          false -> "FAIL -> $record\n" + conditions.joinToString("\n") { it.descriptionFor(record) }
      }
    }
}

