package com.bengangllipson.kafkaclient

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds

data class Payload(
    val key: String, val value: Int
)

class KafkaConsumer {
    suspend fun process() {
        coroutineScope {
            listOf(
                listOf(Payload("a", 1), Payload("b", 2), Payload("a", 3)),
                listOf(Payload("c", 4), Payload("b", 5), Payload("d", 6)),
            ).flatMap { batch ->
                batch.map { i ->
                    async {
                        println("processing $i")
                        delay((0 until 50).random().milliseconds)
                        "processed $i"
                    }
                }.awaitAll()
            }.onEach { result -> println(result) }
        }
    }
}

    suspend fun main() {
        KafkaConsumer().process()
    }
