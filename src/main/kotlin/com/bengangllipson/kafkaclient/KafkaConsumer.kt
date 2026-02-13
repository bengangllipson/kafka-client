@file:OptIn(ExperimentalCoroutinesApi::class)

package com.bengangllipson.kafkaclient

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import kotlin.math.absoluteValue
import kotlin.math.min
import kotlin.time.Duration.Companion.milliseconds

data class WorkerConfiguration<M>(
    val count: Int, val mailboxSize: Int, val selector: (M) -> Int
)

data class Payload(
    val key: String, val value: Int
)

fun <M, K> hashedValueSelector(
    workerCount: Int, partitionBy: (M) -> K, mailboxSize: Int = 5000
): WorkerConfiguration<M> = WorkerConfiguration(
    count = workerCount, mailboxSize = mailboxSize, selector = { value ->
        partitionBy(value).hashCode().absoluteValue.rem(workerCount)
    })

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

fun <T, R> Flow<T>.parallel(
    workers: WorkerConfiguration<T>, capacity: Int = min(
        Int.MAX_VALUE, workers.count * workers.mailboxSize
    ), workerProcessor: suspend (T) -> R
): Flow<R> = channelFlow {
    coroutineScope {
        val workerMailboxes = (0 until workers.count).map {
            Channel<Pair<T, CompletableDeferred<R>>>(workers.mailboxSize)
        }
        val outbox = produce(capacity = capacity) {
            this@parallel.collect {
                val deferred = CompletableDeferred<R>()
                workerMailboxes[workers.selector(it)].send(
                    it to deferred
                )
                send(deferred)
            }
            workerMailboxes.forEach { it.close() }
        }
        workerMailboxes.forEach { mailbox ->
            launch {
                mailbox.consumeEach { (value, deferred) ->
                    deferred.complete(workerProcessor(value))
                }
            }
        }
        outbox.consumeEach { deferred ->
            this@channelFlow.send(deferred.await())
        }
    }
}

class KafkaConsumer {
    suspend fun process() {
        coroutineScope {
            flowOf(
                listOf(Payload("a", 1), Payload("b", 2), Payload("a", 3)),
                listOf(Payload("c", 4), Payload("b", 5), Payload("d", 6)),
            ).flatMapConcat { batch -> batch.asFlow() }.parallel(
                hashedValueSelector(
                    partitionBy = { payload -> payload.key }, workerCount = Runtime.getRuntime().availableProcessors()
                )
            ) {
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
