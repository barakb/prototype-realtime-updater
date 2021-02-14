package com.totango.prototype.realtimeupdater

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.github.michaelbull.retry.policy.fullJitterBackoff
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOffset
import reactor.kafka.receiver.ReceiverOptions
import reactor.kafka.receiver.ReceiverRecord
import reactor.kotlin.core.publisher.toFlux
import java.net.UnknownHostException
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger


@Component
class RealtimeUpdaterPipeline(private val properties: RealtimeUpdaterProperties) {

    val pipelineIdGen = AtomicInteger()

    val consumerProps: Map<String, Any> = properties.kafka.consumer.build()

    private val updatersDispatcher: ExecutorCoroutineDispatcher =
        Executors.newFixedThreadPool(properties.updaterThreads) { r: Runnable -> Thread(r, "Updater") }
            .asCoroutineDispatcher()

    val updaterScheduler: Scheduler = Schedulers.fromExecutorService(updatersDispatcher.executor as ExecutorService)

    private val pipelineScope: CoroutineScope = CoroutineScope(updatersDispatcher)

    private val semaphore: Semaphore = Semaphore(properties.inFlightUpdates)

    private val mutex = Mutex()
    private var pipelines = listOf<Pair<String, Job>>()

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
                found.second.cancel()
                pipelines = pipelines.filter { it != found }
            }
        }
    }

    suspend fun registerPipeline(serviceId: String, job: Job) {
        mutex.withLock {
            pipelines = pipelines + (serviceId to job)
            logger.info("$serviceId pipeline was added")
        }
    }

    suspend fun addService(serviceId: String) {

        val pipelineId = "$serviceId:${pipelineIdGen.incrementAndGet()}"

        val topic = "topic_$serviceId"
        val processor = ReceiverProcessorImpl()

        val receiverOptions: ReceiverOptions<Int, String> = ReceiverOptions.create<Int, String>(consumerProps)
            .subscription(listOf(topic))

        val batchFlux = batchFlux(
            pipelineId, KafkaReceiver.create(receiverOptions).receive(),
            processor,
            properties.batchSize,
            properties.batchMaxDelay
        )

        val job = pipelineScope.launch(updatersDispatcher) {
            batchFlux.asFlow().collect { batch: MutableList<Pair<Payload, ReceiverOffset>> ->
                try {
                    sendBatchWithRetries(pipelineId, batch.map { it.first })
                } catch (e: Exception) {
                    logger.error("$pipelineId: no more retries for batch (${batch.size}) $batch")
                }
                batch.last().second.commit()
            }
        }
        registerPipeline(serviceId, job)
    }

    private fun batchFlux(
        pipelineId: String,
        receiveFlux: Flux<ReceiverRecord<Int, String>>,
        processor: ReceiverProcessor<Payload>,
        batchSize: Int,
        batchMaxDelay: Long,
    ): Flux<MutableList<Pair<Payload, ReceiverOffset>>> =
        receiveFlux
            .publishOn(updaterScheduler)
            .flatMap(processor.process(pipelineId))
            .bufferTimeout(batchSize, Duration.ofSeconds(batchMaxDelay))


    private fun <T : Any> ReceiverProcessor<T>.process(pipelineId: String): (ReceiverRecord<Int, String>) -> Flux<Pair<T, ReceiverOffset>> =
        { record: ReceiverRecord<Int, String> ->
            try {
                logger.debug("$pipelineId: received kafka record: $record")
                processIncomingRecord(parse(record.value())).toFlux().map { (it to record.receiverOffset()) }
            } catch (e: Exception) {
                logger.error("$pipelineId: error while parsing record $record", e)
                Flux.empty()
            }
        }


    private suspend fun sendBatchWithRetries(pipelineId: String, item: List<Payload>) {
        val sendDelay = item[0].sendDelay
        var requestedFails = item[0].retries
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
                    sendBatch(pipelineId, item, sendDelay, shouldFail)
                }
            }catch(e: Exception){
                logger.warn("$pipelineId: send batch (${item.size}) failed with exception $e")
                throw e
            }

        }
    }

    private suspend fun sendBatch(
        pipelineId: String,
        item: List<Payload>,
        sendDelay: Int,
        shouldFail: Boolean
    ) {
        logger.info("--> $pipelineId: sending (${item.size}) [shouldFail=$shouldFail] $item")
        delay(sendDelay * 1000L)
        if (shouldFail) {
            throw UnknownHostException("should fail")
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