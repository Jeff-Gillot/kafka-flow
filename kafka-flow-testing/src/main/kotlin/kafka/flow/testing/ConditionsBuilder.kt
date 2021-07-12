package kafka.flow.testing

public class ConditionsBuilder<Record>() {
    private val conditions = ArrayList<Condition<Record>>()

    public fun condition(description: String, assertionBlock: (Record) -> Unit) {
        conditions.add(Condition(description, assertionBlock))
    }

    public fun build(): Conditions<Record> {
        require(conditions.isNotEmpty()) { "You must have at least one condition" }
        return Conditions(conditions)
    }
}