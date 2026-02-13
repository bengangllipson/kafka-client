@file:OptIn(ExperimentalCoroutinesApi::class)

package com.bengangllipson.kafkaclient

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlin.time.Duration.Companion.milliseconds

data class Payload(
    val key: String, val value: Int
)

fun <T, R> Flow<T>.asyncStep(
    transform: suspend (T) -> R
): Flow<R> = channelFlow {
    coroutineScope {
        produce(capacity = 64) {
            this@asyncStep.collect { value ->
                send(
                    async {
                        transform(value)
                    })
            }
        }.consumeEach {
            this@channelFlow.send(it.await())
        }
    }
}

class KafkaConsumer {
    suspend fun process() {
        coroutineScope {
            flowOf(
                listOf(Payload("a", 1), Payload("b", 2), Payload("a", 3)),
                listOf(Payload("c", 4), Payload("b", 5), Payload("d", 6)),
            ).flatMapConcat { batch -> batch.asFlow() }.asyncStep {
                println("processing $it")
                delay((0 until 50).random().milliseconds)
                "processed $it"
            }.collect { result -> println(result) }
        }
    }
}

suspend fun main() {
    KafkaConsumer().process()
}
