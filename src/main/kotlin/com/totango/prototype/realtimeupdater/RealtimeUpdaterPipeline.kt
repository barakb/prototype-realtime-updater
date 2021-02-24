package com.totango.prototype.realtimeupdater

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.github.michaelbull.retry.policy.fullJitterBackoff
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import reactor.core.Disposables
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOffset
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kotlin.core.publisher.toFlux
import java.net.UnknownHostException
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong


class RealtimeUpdaterPipeline(
    serviceId: String,
    private val pipelineId: String,
    private val consumerProps: Map<String, Any>,
    subscriberScheduler: Scheduler,
    private val updaterCoroutineScope: CoroutineScope,
    private val properties: RealtimeUpdaterProperties,
    private val sendChannel: Channel<suspend () -> Unit>
) {

    private val processor = ReceiverProcessorImpl()

    private val processorTopic = RealtimeUpdaterPipelineManager.processorTopic(serviceId)
    private val updaterTopic = RealtimeUpdaterPipelineManager.updaterTopic(serviceId)

    private val sender: KafkaSender<String, String> =
        KafkaSender.create(SenderOptions.create(properties.kafka.producer.build()))


    private fun <K, V> receivedFrom(topic: String): Flux<ReceiverRecord<K, V>> {
        val options = ReceiverOptions.create<K, V>(consumerProps)
            .subscription(listOf(topic))
        return KafkaReceiver.create(options).receive()
    }

    private val processorFlux = receivedFrom<String, String>(processorTopic)
        .publishOn(subscriberScheduler)
        .concatMap({ record ->
            val processed = processor.processIncomingRecord(mapper.fromString(record.value())) //todo handle bad input
//            logger.info("<-- $pipelineId: [P] processed one record from '$processorTopic' yield ${processed.size} records")
            val records = processed.toFlux()
                .map(mapper::writeValueAsString) //todo handle bad input
                .map { SenderRecord.create(ProducerRecord(updaterTopic, record.key(), it), it) }
            sender.send(records).doOnComplete {
//                logger.info("--> $pipelineId: [P] committing ${processed.size} records to '$updaterTopic'")
                record.receiverOffset().commit()
            }
        }, 1)


    private val updaterFlux = receivedFrom<String, String>(updaterTopic)
        .publishOn(subscriberScheduler)
//        .doOnNext { logger.info("<-- $pipelineId: [U] read one record from '$updaterTopic'") }
        .map { record -> (mapper.fromString<Payload>(record.value()) to record.receiverOffset()) }
        .bufferTimeout(properties.batchSize, Duration.ofSeconds(properties.batchMaxDelay))
        .map { Batch(it) }
        .doOnNext { logger.info("    $pipelineId: [U] collect to $it") }
        .concatMap(this::sendBatchAsMono, 1)

    fun activate(): Disposable {
        return Disposables.composite(processorFlux.subscribe(), updaterFlux.subscribe())
    }

    private fun sendBatchAsMono(batch: Batch) = Mono.create<Unit> { sink ->
        val done: CompletableDeferred<Unit> = CompletableDeferred()
        updaterCoroutineScope.launch {
            try {
                sendChannel.send {
                    sendAndCommit(batch, done)
                }
                sink.success(done.await())
            } catch (e: Exception) {
                logger.error("unexpected error $e while sending $batch", e)
                sink.success(Unit)
            }
        }
    }

    private suspend fun sendAndCommit(batch: Batch, done: CompletableDeferred<Unit>) {
        try {
            sendBatchWithRetries(batch)
        } catch (e: Exception) {
            logger.error("    $pipelineId: [U] no more retries for $batch")
        }
        logger.info("    $pipelineId: [U] Committing $batch")
        batch.commit()
        done.complete(Unit)
    }

    private suspend fun sendBatchWithRetries(batch: Batch) {
        var requestedFails = batch.requestedFails
        retry(
            limitAttempts(properties.sendRetries) + fullJitterBackoff(
                base = 10L,
                max = properties.sendRetryMaxDelay
            )
        ) {
            val shouldFail = 0 < requestedFails
            requestedFails--
            try {
                justSendBatch(batch.copy(shouldFail = shouldFail))
            } catch (e: Exception) {
                logger.warn("    $pipelineId: [U] sending of $batch failed with exception $e")
                throw e
            }

        }
    }

    private suspend fun justSendBatch(batch: Batch) {
        logger.info("--> $pipelineId: [U] sending $batch [shouldFail=${batch.shouldFail}]")
        if (0 < batch.sendDelay) {
            delay(batch.sendDelay.toLong())
        }
        if (batch.shouldFail) {
            logger.info("    $pipelineId: [U] send $batch failed.")
            throw UnknownHostException("should fail")
        } else {
            logger.info("    $pipelineId: [U] send $batch succeed.")
        }
    }


    private inline fun <reified T : Any> ObjectMapper.fromString(value: String): T {
        @Suppress("BlockingMethodInNonBlockingContext")
        return this.readValue(value, T::class.java)
    }

    companion object {
        @Suppress("unused")
        val logger: Logger = LoggerFactory.getLogger(RealtimeUpdaterPipeline::class.java)
        val mapper: ObjectMapper = ObjectMapper().registerModule(KotlinModule())
    }
}

val batchId = AtomicLong()

data class Batch(
    private val items: List<Pair<Payload, ReceiverOffset>>,
    val shouldFail: Boolean = false
) : List<Payload> by (items.map { it.first }) {
    private val id = batchId.getAndIncrement()
    val sendDelay = this[0].sendDelay
    val requestedFails = this[0].retries

    fun commit(): Mono<Void> = items.last().second.commit()

    override fun toString(): String = "Batch $id size=${items.size}"
}