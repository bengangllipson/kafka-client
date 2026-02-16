@file:OptIn(ExperimentalCoroutinesApi::class)

package com.bengangllipson.kafkaclient

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flowOn
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlin.math.absoluteValue
import kotlin.math.min

typealias ProcessingStep<T> = Pair<InputMetadata, T>

@Serializable
data class ParsedPayload(
    val tcin: String,
    val locationId: String,
    val onHandQuantity: List<Event>,
    val onPurchaseQuantity: List<Event>,
    val onTransferQuantity: List<Event>
) {
    @Serializable
    data class Event(
        val quantity: Int
    )
}

data class TransformedResult(
    val tcin: String, val locationId: String, val quantity: Quantity
) {
    data class Quantity(
        val onHand: Int, val onPurchase: Int, val onTransfer: Int
    )
}

data class WorkerConfiguration<M>(
    val count: Int, val mailboxSize: Int, val selector: (M) -> Int
)

data class Payload(
    val key: String, val body: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Payload

        if (key != other.key) return false
        if (!body.contentEquals(other.body)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + body.contentHashCode()
        return result
    }
}

data class InputMetadata(
    val topic: String, val key: String, val partitionOffset: Pair<Int, Long>, val isEndOfBatch: Boolean
)

private val consumerThread = Executors.newSingleThreadExecutor { r ->
    Thread(r, "kafka-consumer")
}.asCoroutineDispatcher()

sealed interface State<out T>

@JvmInline
value class Success<T>(val value: T) : State<T>
data object FilteredMessage : State<Nothing>

fun ProcessingStep<Payload>.filter(): ProcessingStep<State<Payload>> {
    val (metadata, payload) = this
    val state: State<Payload> =
        payload.takeIf { it.key.split(":").first().toIntOrNull() != null }?.let { Success(it) } ?: FilteredMessage
    return metadata to state
}

fun ProcessingStep<State<Payload>>.parse() = this.let { (metadata, state) ->
    Pair(
        metadata, when (state) {
            is FilteredMessage -> state
            is Success -> {
                Success(Json.decodeFromString<ParsedPayload>(state.value.body.decodeToString()))
            }
        }
    )
}

fun ProcessingStep<State<ParsedPayload>>.transform(): ProcessingStep<State<TransformedResult>> =
    this.let { (metadata, state) ->
        Pair(
            metadata, when (state) {
            is FilteredMessage -> state
            is Success -> {
                with(state.value) {
                    TransformedResult(
                        tcin = tcin,
                        locationId = locationId,
                        quantity = TransformedResult.Quantity(
                            onHand = onHandQuantity.sumOf { it.quantity },
                            onPurchase = onPurchaseQuantity.sumOf { it.quantity },
                            onTransfer = onTransferQuantity.sumOf { it.quantity })
                    )
                }.let { Success(it) }
            }
        })
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

fun createStringConsumer(bootstrapServers: String, groupId: String): KafkaConsumer<String, String> {
    val props = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"
        )
        put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"
        )
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    }
    return KafkaConsumer(props)
}

class KafkaConsumer {
    suspend fun process() {
        val consumer = createStringConsumer("localhost:9092", "kafka-transformer")
        consumer.subscribe(listOf("local-test-topic"))

        suspend fun ProcessingStep<State<TransformedResult>>.commitOffsets() = this.also { (metadata, _) ->
            withContext(consumerThread) {
                if (metadata.isEndOfBatch) {
                    val (partition, offset) = metadata.partitionOffset
                    consumer.commitSync(
                        mapOf(
                            TopicPartition(
                                metadata.topic, partition
                            ) to OffsetAndMetadata(offset + 1)
                        )
                    )
                }
            }
        }

        val kafkaMessages = channelFlow {
            consumer.poll(Duration.ofSeconds(30)).let { records ->
                val batchSize = records.count()
                records.forEachIndexed { index, record ->
                    send(
                        InputMetadata(
                            topic = record.topic(), key = record.key(), partitionOffset = Pair(
                                record.partition(), record.offset()
                            ), isEndOfBatch = index + 1 == batchSize
                        ) to Payload(
                            key = record.key(), body = record.value().toByteArray()
                        )
                    )
                }
            }.also { yield() }
        }.flowOn(consumerThread)

        val workerConfiguration = WorkerConfiguration<ProcessingStep<Payload>>(
            count = 100, mailboxSize = 5000, selector = { (metadata: InputMetadata, _) ->
                metadata.key.hashCode().absoluteValue.rem(100)
            })

        kafkaMessages.parallel(workerConfiguration) {
                it.filter().parse().transform()
            }.collect { it.commitOffsets() }
    }
}

suspend fun main() {
    KafkaConsumer().process()
}
