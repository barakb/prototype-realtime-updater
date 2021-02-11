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

@Component
class RealtimeUpdaterPipeline(private val properties: RealtimeUpdaterProperties) {

    val consumerProps: Map<String, Any> = properties.kafka.consumer.build()

    private final val subscriberDispatcher: ExecutorCoroutineDispatcher =
        Executors.newFixedThreadPool(properties.subscriberThreads) { r: Runnable ->
            Thread(r, "Subscriber")
        }.asCoroutineDispatcher()

    private val updatersDispatcher: ExecutorCoroutineDispatcher =
        Executors.newFixedThreadPool(properties.updaterThreads) { r: Runnable -> Thread(r, "Updater") }
            .asCoroutineDispatcher()

    private val pipelineScope: CoroutineScope =
        CoroutineScope(Executors.newFixedThreadPool(1) { r: Runnable -> Thread(r, "Pipeline") }
            .asCoroutineDispatcher())

    private val semaphore: Semaphore = Semaphore(properties.inFlightUpdates)

    private val mutex = Mutex()
    private var pipelines = listOf<Pair<String, Job>>()

    fun list(): List<String> {
        return pipelines.map {
            it.first
        }
    }

    suspend fun removeService(service: String) {
        logger.info("removing service $service from pipeline")
        mutex.withLock {
            val found = pipelines.firstOrNull { it.first == service }
            if (found != null) {
                found.second.cancel()
                pipelines = pipelines.filter { it != found }
            }
        }
    }

    @FlowPreview
    @ExperimentalCoroutinesApi
    @ObsoleteCoroutinesApi
    suspend fun addService(serviceId: String) {

        val job = pipelineScope.launch {
            val topic = "topic_$serviceId"


            val receiverOptions: ReceiverOptions<Int, String> = ReceiverOptions.create<Int, String>(consumerProps)
                .subscription(listOf(topic))

            val updater: UpdateBuffer<Payload> = UpdateBuffer(properties.batchSize)

            suspend fun UpdateBuffer<Payload>.flush() {
                val items = returnAndFlush()
                if (items != null) batchHandlingAuxFun(items)
            }

            val processor = ReceiverProcessorImpl()

            val payloadFlow: Flow<Either.Right<Pair<Payload, ReceiverOffset>>> =
                KafkaReceiver.create(receiverOptions).receive()
                    .asFlow()
                    .flowOn(subscriberDispatcher)
                    .flatMapConcat { record ->
                        // parse from string, process with processor and wrap with right
                        processor.processIncomingRecord(parse(record.value()))
                            .asFlow()
                            .map { Either.right(it to record.receiverOffset()) }
                    }.catch { cause: Throwable ->
                        logger.error("failed to process record ", cause)
                    }
            val timeoutsFlow: Flow<Either.Left<Long>> =
                Flux.interval(Duration.ofMillis(properties.batchMaxDelay))
                    .asFlow()
                    .map { Either.left(it) }

            val payloadOrTimeout: Flow<Either<Long, Pair<Payload, ReceiverOffset>>> =
                listOf(payloadFlow, timeoutsFlow)
                    .merge()
                    .onEach { item ->
                        when (item) {
                            is Either.Left -> updater.flush()
                            is Either.Right -> {
                                val (payload: Payload, offset: ReceiverOffset) = item.right
                                addToBatchUpdateIfExceeds(updater, payload, offset)
                            }
                        }
                    }.catch { e: Throwable ->
                        logger.error("fail to process update", e)
                    }.flowOn(updatersDispatcher)

            launch(updatersDispatcher) {
                payloadOrTimeout.collect()
            }

        }

        mutex.withLock {
            pipelines = pipelines + (serviceId to job)
            logger.info("$serviceId was added to pipeline")
        }
    }

    private suspend fun addToBatchUpdateIfExceeds(
        updater: UpdateBuffer<Payload>,
        item: Payload,
        receiverOffset: ReceiverOffset
    ) {
        val updateBatch = updater.updateBatch(item, receiverOffset)
        if (updateBatch != null && updateBatch.isNotEmpty()) {
            batchHandlingAuxFun(updateBatch)
        }
    }


    private suspend fun batchHandlingAuxFun(item: List<Payload>) {
        val batchCommander = item[0]
        do {
            semaphore.withPermit {
                delay(batchCommander.sendDelay * 1000L)
                logger.info("--> sending $item")
                if (batchCommander.retries > 0) {
                    logger.info("send fail, will retry in ${properties.retryDelay} seconds.")
                    delay(properties.retryDelay * 1000L)
                } else {
                    logger.info("send done.")
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