public fun <O> (() -> O).invokeAndThrow(): O = try {
    invoke()
} catch (throwable: Throwable) {
    throwable.printStackTrace()
    throw throwable
}

public fun <I, O> ((I) -> O).invokeAndThrow(input: I): O = try {
    invoke(input)
} catch (throwable: Throwable) {
    throwable.printStackTrace()
    throw throwable
}

public suspend fun <O> (suspend () -> O).invokeAndThrow(): O = try {
    invoke()
} catch (throwable: Throwable) {
    throwable.printStackTrace()
    throw throwable
}

public suspend fun <I, O> (suspend (I) -> O).invokeAndThrow(input: I): O = try {
    invoke(input)
} catch (throwable: Throwable) {
    throwable.printStackTrace()
    throw throwable
}

public fun <I1, I2, O> ((I1, I2) -> O).invokeAndThrow(input1: I1, input2: I2): O = try {
    invoke(input1, input2)
} catch (throwable: Throwable) {
    throwable.printStackTrace()
    throw throwable
}

public suspend fun <I1, I2, O> (suspend (I1, I2) -> O).invokeAndThrow(input1: I1, input2: I2): O = try {
    invoke(input1, input2)
} catch (throwable: Throwable) {
    throwable.printStackTrace()
    throw throwable
}