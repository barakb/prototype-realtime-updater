package com.totango.prototype.realtimeupdater

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.Disposable
import reactor.core.scheduler.Schedulers
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

@Component
class RealtimeUpdaterPipelineManager(private val properties: RealtimeUpdaterProperties) {
    private val pipelineIdGen = AtomicInteger()

    private val consumerProps: Map<String, Any> = properties.kafka.consumer.build()

    private val subscriberScheduler = Schedulers.fromExecutorService(Executors.newFixedThreadPool(2) {
        Thread(it, "subscriber")
    }, "subscriber")

    private val updaterCoroutineContext =
        (Executors.newFixedThreadPool(1) { Thread(it, "updater-thread") }.asCoroutineDispatcher()
                + CoroutineName("updater"))

    private val updaterCoroutineScope = CoroutineScope(updaterCoroutineContext)

    private val sendChannel = Channel<suspend () -> Unit>()
    private val mutex = Mutex()
    private var pipelines = listOf<Pair<String, Disposable>>()


    init {
        repeat(properties.inFlightUpdates) {
            updaterCoroutineScope.launch {
                for (action in sendChannel) {
                    try {
                        action()
                    } catch (e: Exception) {
                        logger.error("Error while executing action", e)
                    }
                }
            }
        }
    }

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

    suspend fun createPipeline(serviceId: String) {
        val pipelineId = "$serviceId:${pipelineIdGen.incrementAndGet()}"
        val pipeline =
            RealtimeUpdaterPipeline(
                serviceId,
                pipelineId,
                consumerProps,
                subscriberScheduler,
                updaterCoroutineScope,
                properties,
                sendChannel
            )
        val disposable: Disposable = pipeline.activate()
        registerPipeline(serviceId, disposable)
    }

    private suspend fun registerPipeline(serviceId: String, disposable: Disposable) {
        mutex.withLock {
            pipelines = pipelines + (serviceId to disposable)
            logger.info("$serviceId pipeline was added")
        }
    }

    companion object {
        @Suppress("unused")
        val logger: Logger = LoggerFactory.getLogger(RealtimeUpdaterPipelineManager::class.java)
        fun processorTopic(serviceId: String): String = "topic_processor_$serviceId"
        fun updaterTopic(serviceId: String): String = "topic_updater_$serviceId"
    }
}
