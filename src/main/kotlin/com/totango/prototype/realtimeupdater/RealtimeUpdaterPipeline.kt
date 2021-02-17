package com.totango.prototype.realtimeupdater

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.github.michaelbull.retry.policy.fullJitterBackoff
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.MonoSink
import reactor.core.scheduler.Schedulers
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOffset
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kotlin.core.publisher.toFlux
import java.net.UnknownHostException
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger


@Component
class RealtimeUpdaterPipeline(private val properties: RealtimeUpdaterProperties) {

    private val pipelineIdGen = AtomicInteger()

    private val consumerProps: Map<String, Any> = properties.kafka.consumer.build()

    private val subscriberScheduler = Schedulers.newSingle("subscriber")

    private val updaterCoroutineContext =
        (Executors.newFixedThreadPool(1) { Thread(it, "updater-thread") }.asCoroutineDispatcher()
                + CoroutineName("updater"))

    private val updaterCoroutineScope = CoroutineScope(updaterCoroutineContext)

    private val semaphore: Semaphore = Semaphore(properties.inFlightUpdates)

    private val mutex = Mutex()
    private var pipelines = listOf<Pair<String, Disposable>>()

    fun list(): List<String> {
        return pipelines.map {
            it.first
        }
    }

    suspend fun removePipeline(service: String) {
        logger.info("removing pipeline $service")
        mutex.withLock {
            val found = pipelines.firstOrNull { it.first == service }
            if (found != null) {
                logger.info("disposing ${found.second.dispose()}")
                found.second.dispose()
                pipelines = pipelines.filter { it != found }
            }
        }
    }

    suspend fun registerPipeline(serviceId: String, disposable: Disposable) {
        mutex.withLock {
            pipelines = pipelines + (serviceId to disposable)
            logger.info("$serviceId pipeline was added")
        }
    }

    suspend fun addService(serviceId: String) {

        val pipelineId = "$serviceId:${pipelineIdGen.incrementAndGet()}"

        val topic = "topic_$serviceId"
        val processor = ReceiverProcessorImpl()

        fun process(records: Flux<ReceiverRecord<Int, String>>): Flux<Pair<Payload, ReceiverOffset>> =
            records.concatMap({ record ->
                try {
                    logger.debug("<-- $pipelineId: received a kafka record")
                    processor.processIncomingRecord(parse(record.value())).toFlux()
                        .map { (it to record.receiverOffset()) }
                } catch (e: Exception) {
                    logger.error("$pipelineId: error while parsing record $record", e)
                    Flux.empty()
                }
            }, 1)

        val receiverOptions: ReceiverOptions<Int, String> = ReceiverOptions.create<Int, String>(consumerProps)
            .subscription(listOf(topic))
        logger.info("receiverOptions: $receiverOptions")


        val receiver = KafkaReceiver.create(receiverOptions)
        val batchFlux = receiver.receive()
            .publishOn(subscriberScheduler)
            .transform{process(it)}
            .bufferTimeout(properties.batchSize, Duration.ofSeconds(properties.batchMaxDelay))
            .concatMap (suspendedToMono {
                sendAndCommit(pipelineId, it)
            }, 1)

        val disposable: Disposable = batchFlux.subscribe()
        registerPipeline(serviceId, disposable)
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


    private suspend fun sendAndCommit(pipelineId: String, batch: List<Pair<Payload, ReceiverOffset>>) {
        val receiverOffset = batch.last().second
        try {
            val items = batch.map { it.first }
            val param = SendBatchParams(pipelineId, items, items[0].sendDelay, items[0].retries)
            sendBatchWithRetries(param)
        } catch (e: Exception) {
            logger.error("    $pipelineId: no more retries for batch (${batch.size}) $batch")
        }
        logger.info("    $pipelineId: Committing batch (${batch.size}) at ${receiverOffset.offset()}")
        receiverOffset.commit()
    }

    private suspend fun sendBatchWithRetries(params: SendBatchParams) {
        var requestedFails = params.requestedFails
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
                    justSendBatch(params.copy(shouldFail = shouldFail))
                }
            } catch (e: Exception) {
                logger.warn("    ${params.pipelineId}: send batch (${params.items.size}) failed with exception $e")
                throw e
            }

        }
    }

    private suspend fun justSendBatch(params: SendBatchParams) {
        logger.info("--> ${params.pipelineId}: sending batch of size (${params.items.size}) [shouldFail=${params.shouldFail}]")
        delay(params.sendDelay * 1000L)
        if (params.shouldFail) {
            logger.info("    ${params.pipelineId}: send batch of size (${params.items.size}) failed.")
            throw UnknownHostException("should fail")
        }else{
            logger.info("    ${params.pipelineId}: send batch of size (${params.items.size}) succeed.")
        }
    }

    private inline fun <reified T : Any> parse(value: String): T {
        @Suppress("BlockingMethodInNonBlockingContext")
        return mapper.readValue(value, T::class.java)
    }

    companion object {
        @Suppress("unused")
        val logger: Logger = LoggerFactory.getLogger(RealtimeUpdaterPipeline::class.java)
        val mapper: ObjectMapper = ObjectMapper().registerModule(KotlinModule())
    }
}

data class SendBatchParams(
    val pipelineId: String,
    val items: List<Payload>,
    val sendDelay: Int,
    val requestedFails: Int,
    val shouldFail: Boolean = false
)