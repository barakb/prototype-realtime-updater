package com.totango.prototype.realtimeupdater

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.sync.withPermit
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOffset
import reactor.kafka.receiver.ReceiverOptions
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

@Component
class RealtimeUpdaterPipeline(private val properties: RealtimeUpdaterProperties) {

    val pipelineIdGen = AtomicInteger()

    val consumerProps: Map<String, Any> = properties.kafka.consumer.build()

    private final val subscriberDispatcher: ExecutorCoroutineDispatcher =
        Executors.newFixedThreadPool(properties.subscriberThreads) { r: Runnable ->
            Thread(r, "Subscriber")
        }.asCoroutineDispatcher()

    private val updatersDispatcher: ExecutorCoroutineDispatcher =
        Executors.newFixedThreadPool(properties.updaterThreads) { r: Runnable -> Thread(r, "Updater") }
            .asCoroutineDispatcher()

    private val pipelineScope: CoroutineScope = CoroutineScope(updatersDispatcher)


    private val semaphore: Semaphore = Semaphore(properties.inFlightUpdates)

    private val mutex = Mutex()
    private var pipelines = listOf<Pair<String, Job>>()

    val timeoutsFlow: Flow<Either.Left<Long>> =
        Flux.interval(Duration.ofMillis(properties.batchMaxDelay))
            .asFlow()
            .map { Either.left(it) }

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

    @FlowPreview
    @ExperimentalCoroutinesApi
    @ObsoleteCoroutinesApi
    suspend fun addService(serviceId: String) {

        val pipelineId = "$serviceId:${pipelineIdGen.incrementAndGet()}"

        val topic = "topic_$serviceId"

        val receiverOptions: ReceiverOptions<Int, String> = ReceiverOptions.create<Int, String>(consumerProps)
            .subscription(listOf(topic))

        val updater: UpdateBuffer<Payload> = UpdateBuffer(properties.batchSize)


        suspend fun UpdateBuffer<Payload>.flush() {
            val items = returnAndFlush()
            if (items != null) batchHandlingAuxFun(pipelineId, items)
        }

        val processor = ReceiverProcessorImpl()

        val payloadFlow: Flow<Either.Right<Pair<Payload, ReceiverOffset>>> =
            KafkaReceiver.create(receiverOptions).receive()
                .asFlow()
                .flowOn(subscriberDispatcher)
                .flatMapConcat { record ->
                    logger.debug("$pipelineId: received kafka record: $record")
                    // parse from string, process with processor and wrap with right
                    processor.processIncomingRecord(parse(record.value()))
                        .asFlow()
                        .map { Either.right(it to record.receiverOffset()) }
                }.catch { cause: Throwable ->
                    logger.error("$pipelineId: failed to process record ", cause)
                }

        val payloadOrTimeout: Flow<Either<Long, Pair<Payload, ReceiverOffset>>> =
            listOf(payloadFlow, timeoutsFlow)
                .merge()
                .onEach { item ->
                    when (item) {
                        is Either.Left -> updater.flush()
                        is Either.Right -> {
                            val (payload: Payload, offset: ReceiverOffset) = item.right
                            addToBatchUpdateIfExceeds(pipelineId, updater, payload, offset)
                        }
                    }
                }.catch { e: Throwable ->
                    logger.error("$pipelineId: fail to process update", e)
                }.flowOn(updatersDispatcher)

        val job = pipelineScope.launch(updatersDispatcher) {
            payloadOrTimeout.collect()
        }

        registerPipeline(serviceId, job)
    }

    private suspend fun addToBatchUpdateIfExceeds(
        pipelineId: String,
        updater: UpdateBuffer<Payload>,
        item: Payload,
        receiverOffset: ReceiverOffset
    ) {
        val updateBatch: List<Payload>? = updater.updateBatch(pipelineId, item, receiverOffset)
        if (updateBatch != null && updateBatch.isNotEmpty()) {
            batchHandlingAuxFun(pipelineId, updateBatch)
        }
    }

    private suspend fun batchHandlingAuxFun(pipelineId: String, item: List<Payload>) {
        val batchCommander = item[0]
        do {
            semaphore.withPermit {
                delay(batchCommander.sendDelay * 1000L)
                logger.info("--> $pipelineId: sending $item")
                if (batchCommander.retries > 0) {
                    logger.info("$pipelineId: send fail, will retry in ${properties.retryDelay} seconds.")
                    delay(properties.retryDelay * 1000L)
                } else {
                    logger.info("$pipelineId: send done.")
                }
            }
            batchCommander.retries--

        } while (batchCommander.retries > -1)
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