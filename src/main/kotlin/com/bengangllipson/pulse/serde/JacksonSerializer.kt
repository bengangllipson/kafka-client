package com.bengangllipson.pulse.serde

import org.apache.kafka.common.serialization.Serializer

class JacksonSerializer<T> : Serializer<T> {
    override fun serialize(
        topic: String?,
        data: T?,
    ): ByteArray? = data?.let { mapper.writeValueAsBytes(it) }
}