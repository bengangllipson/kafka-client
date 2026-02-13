package com.bengangllipson.kafkaclient

import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds

class KafkaConsumer {
    suspend fun process() {
        listOf(1, 2, 3)
            .map { i ->
                println("processing $i")
                delay((0 until 50).random().milliseconds)
                "processed $i"
            }
            .onEach { result -> println(result) }
    }
}

suspend fun main() {
    KafkaConsumer().process()
}
