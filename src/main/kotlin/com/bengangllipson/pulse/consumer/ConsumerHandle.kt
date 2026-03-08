package com.bengangllipson.pulse.consumer

import kotlinx.coroutines.Job

data class ConsumerHandle(
    val job: Job,
    val stop: suspend () -> Unit
)
