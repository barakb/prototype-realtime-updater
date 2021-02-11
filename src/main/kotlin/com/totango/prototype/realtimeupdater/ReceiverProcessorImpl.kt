package com.totango.prototype.realtimeupdater

import kotlin.random.Random

class ReceiverProcessorImpl : ReceiverProcessor<Payload> {
    override fun processIncomingRecord(payload: Payload): List<Payload> {
        return List(Random.nextInt(1, 5)) { payload.copy(info = "${payload.info}_${it}") }
    }
}