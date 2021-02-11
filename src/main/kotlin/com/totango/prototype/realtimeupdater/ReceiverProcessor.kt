package com.totango.prototype.realtimeupdater

interface ReceiverProcessor<T> {
     fun processIncomingRecord(payload: Payload): List<T>
}