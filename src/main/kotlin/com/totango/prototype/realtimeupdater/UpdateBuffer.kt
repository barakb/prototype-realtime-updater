package com.totango.prototype.realtimeupdater

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.kafka.receiver.ReceiverOffset

class UpdateBuffer<T>(private val threshold: Int) {

    private var batch: MutableList<T> = mutableListOf()
    private var receiverOffset: ReceiverOffset? = null

    fun returnAndFlush(): List<T>? {
        return if (batch.isEmpty()) {
            null
        } else {
            val returnedBatch = batch
            batch = mutableListOf()
            commitBatchAndResetOffset()
            returnedBatch
        }
    }

    fun updateBatch(record: T, maxReceiverOffset: ReceiverOffset): List<T>? {

        batch.add(record)
        receiverOffset = maxReceiverOffset

        logger.info("add record $record to batch ${batch.size}")

        return if (batch.size >= threshold) {
            val returnedBatch = batch
            batch = mutableListOf()
            commitBatchAndResetOffset()
            returnedBatch.toList()
        } else {
            null
        }

    }

    @Suppress("UnassignedFluxMonoInstance")
    private fun commitBatchAndResetOffset() {
        receiverOffset?.commit()
        receiverOffset = null
    }

    companion object {
        @Suppress("unused")
        val logger: Logger = LoggerFactory.getLogger(UpdateBuffer::class.java)
    }

}
