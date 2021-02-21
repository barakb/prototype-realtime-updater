package com.totango.prototype.realtimeupdater

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.github.michaelbull.retry.policy.fullJitterBackoff
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.MonoSink
import reactor.core.scheduler.Scheduler
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOffset
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kotlin.core.publisher.toFlux
import java.net.UnknownHostException
import java.time.Duration


class RealtimeUpdaterPipeline(
    serviceId: String,
    private val pipelineId: String,
    consumerProps: Map<String, Any>,
    subscriberScheduler: Scheduler,
    private val updaterCoroutineScope: CoroutineScope,
    private val properties: RealtimeUpdaterProperties,
    private val semaphore: Semaphore
) {

    private val receiverOptions: ReceiverOptions<Int, String> = ReceiverOptions.create<Int, String>(consumerProps)
        .subscription(listOf("topic_$serviceId"))

    private val receiver: KafkaReceiver<Int, String> = KafkaReceiver.create(receiverOptions)

    private val processor = ReceiverProcessorImpl()

    private val batchFlux = receiver.receive()
        .publishOn(subscriberScheduler)
        .transform { process(it) }
        .bufferTimeout(properties.batchSize, Duration.ofSeconds(properties.batchMaxDelay))

        .concatMap(suspendedToMono {
            sendAndCommit(it)
        }, 1)

    private fun process(records: Flux<ReceiverRecord<Int, String>>): Flux<Pair<Payload, ReceiverOffset>> =
        records.concatMap({ record ->
            try {
                logger.debug("<-- $pipelineId: received a kafka record")
                processor.processIncomingRecord(mapper.fromString(record.value())).toFlux()
                    .map { (it to record.receiverOffset()) }
            } catch (e: Exception) {
                logger.error("$pipelineId: error while parsing record $record", e)
                Flux.empty()
            }
        }, 1)


    fun activate(): Disposable {
        return batchFlux.subscribe()
    }

    private fun <A, B> suspendedToMono(block: suspend (A) -> B): (A) -> Mono<B> = { a ->
        Mono.create { sink: MonoSink<B> ->
            updaterCoroutineScope.launch {
                try {
                    sink.success(block(a))
                } catch (e: Exception) {
                    sink.error(e)
                }
            }
        }
    }


    private suspend fun sendAndCommit(batch: List<Pair<Payload, ReceiverOffset>>) {
        val receiverOffset = batch.last().second
        try {
            val items = batch.map { it.first }
            val param = Batch(items, items[0].sendDelay, items[0].retries)
            sendBatchWithRetries(param)
        } catch (e: Exception) {
            logger.error("    $pipelineId: no more retries for batch (${batch.size}) $batch")
        }
        logger.info("    $pipelineId: Committing batch (${batch.size}) at ${receiverOffset.offset()}")
        receiverOffset.commit()
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
                semaphore.withPermit {
                    justSendBatch(batch.copy(shouldFail = shouldFail))
                }
            } catch (e: Exception) {
                logger.warn("    $pipelineId: send batch (${batch.size}) failed with exception $e")
                throw e
            }

        }
    }

    private suspend fun justSendBatch(batch: Batch) {
        logger.info("--> $pipelineId: sending batch of size (${batch.size}) [shouldFail=${batch.shouldFail}]")
        delay(batch.sendDelay * 1000L)
        if (batch.shouldFail) {
            logger.info("    $pipelineId: send batch of size (${batch.size}) failed.")
            throw UnknownHostException("should fail")
        } else {
            logger.info("    $pipelineId: send batch of size (${batch.size}) succeed.")
        }
    }


    private inline fun <reified  T: Any> ObjectMapper.fromString(value: String): T{
        @Suppress("BlockingMethodInNonBlockingContext")
        return this.readValue(value, T::class.java)
    }

    companion object {
        @Suppress("unused")
        val logger: Logger = LoggerFactory.getLogger(RealtimeUpdaterPipeline::class.java)
        val mapper: ObjectMapper = ObjectMapper().registerModule(KotlinModule())
    }
}

data class Batch(
    private val items: List<Payload>,
    val sendDelay: Int,
    val requestedFails: Int,
    val shouldFail: Boolean = false
) : List<Payload> by items