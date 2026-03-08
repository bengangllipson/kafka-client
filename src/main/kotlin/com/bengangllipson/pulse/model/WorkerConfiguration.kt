package com.bengangllipson.pulse.model

data class WorkerConfiguration<M>(
    val count: Int, val mailboxSize: Int, val selector: (M) -> Int
)
