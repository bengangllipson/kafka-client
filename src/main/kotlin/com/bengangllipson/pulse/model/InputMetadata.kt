package com.bengangllipson.pulse.model

data class InputMetadata(
    val topic: String, val key: String, val partitionOffset: Pair<Int, Long>, val isEndOfBatch: Boolean
)
