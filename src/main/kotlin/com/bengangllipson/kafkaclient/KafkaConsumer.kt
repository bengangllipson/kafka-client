package com.bengangllipson.kafkaclient

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds

class KafkaConsumer {
    suspend fun process() {
        coroutineScope {
            listOf(1, 2, 3)
                .map { i ->
                    async {
                        println("processing $i")
                        delay((0 until 50).random().milliseconds)
                        "processed $i"
                    }
                }
                .awaitAll()
                .onEach { result -> println(result) }
        }
    }
}

suspend fun main() {
    KafkaConsumer().process()
}
