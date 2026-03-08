package com.bengangllipson.pulse.producer

import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors

val producerThread =
    Executors.newSingleThreadExecutor { r -> Thread(r, "kafka-producer") }.asCoroutineDispatcher()
