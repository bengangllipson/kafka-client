package com.bengangllipson.pulse.producer

import com.bengangllipson.pulse.model.WorkerConfiguration
import kotlinx.coroutines.flow.flowOf
import kotlinx.serialization.Serializable
import org.apache.kafka.clients.producer.ProducerRecord
import kotlin.math.absoluteValue

@Serializable
data class MyEvent(
    val itemId: String,
    val locationId: String,
    val onHandQuantity: Int,
    val onPurchaseQuantity: Int,
    val onTransferQuantity: Int
)

class Example {
    val mapper: (MyEvent) -> ProducerRecord<String, MyEvent> = { myEvent ->
        ProducerRecord(
            "my-topic", myEvent.itemId, myEvent
        )
    }

    val onError: (Throwable) -> Unit = { throwable ->
        println("Consumer error: ${throwable.message}")
        throwable.printStackTrace()
    }

    val producer = Producer.Builder(
        config = Producer.Config(
            appName = "example",
            broker = "localhost:9092",
            topic = "my-topic",
            workerConfig = WorkerConfiguration(
                count = 100, mailboxSize = 5000, selector = { myEvent ->
                    myEvent.itemId.hashCode().absoluteValue.rem(100)
                }),
        ), recordMapper = mapper, onError = onError
    ).build()

    fun start() {
        val input = flowOf(
            MyEvent("1", "12", 5, 4, 3),
            MyEvent("2", "13", 5, 4, 3),
            MyEvent("3", "14", 5, 4, 3),
        )
        producer.start(input)
    }
}

fun main() {
    Example().start()
}
