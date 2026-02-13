@file:OptIn(ExperimentalCoroutinesApi::class)

package com.bengangllipson.kafkaclient

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlin.time.Duration.Companion.milliseconds

data class Payload(
    val key: String, val value: Int
)

class KafkaConsumer {
    suspend fun process() {
        coroutineScope {
            flowOf(
                listOf(Payload("a", 1), Payload("b", 2), Payload("a", 3)),
                listOf(Payload("c", 4), Payload("b", 5), Payload("d", 6)),
            ).flatMapConcat { batch -> batch.asFlow() }.map { i ->
                async {
                    println("processing $i")
                    delay((0 until 50).random().milliseconds)
                    "processed $i"
                }
            }.map { i -> i.await() }
                .collect { result -> println(result) }
        }
    }
}

suspend fun main() {
    KafkaConsumer().process()
}
