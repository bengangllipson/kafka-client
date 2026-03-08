package com.bengangllipson.pulse.consumer

import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors

val consumerThread = Executors.newSingleThreadExecutor { r ->
    Thread(r, "kafka-consumer")
}.asCoroutineDispatcher()
