package com.bengangllipson.pulse

import kotlinx.coroutines.Job

data class Handle(
    val job: Job,
    val stop: suspend () -> Unit
)