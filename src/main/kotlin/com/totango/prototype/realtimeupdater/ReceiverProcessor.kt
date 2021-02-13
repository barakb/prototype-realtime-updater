package com.totango.prototype.realtimeupdater

interface ReceiverProcessor<T : Any> {
     fun processIncomingRecord(payload: Payload): List<T>
}