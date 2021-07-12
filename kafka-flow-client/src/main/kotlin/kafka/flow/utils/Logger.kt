package kafka.flow.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory

public inline fun <reified T> T.logger(): Logger = LoggerFactory.getLogger(T::class.java)