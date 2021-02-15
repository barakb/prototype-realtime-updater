package com.totango.prototype.realtimeupdater

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.swagger.v3.oas.annotations.Operation
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Flux
import java.util.*
import javax.validation.constraints.Min

@Suppress("unused")
@RestController
@RequestMapping("/api")
class ApiEndpoint (private val pipeline: RealtimeUpdaterPipeline, private val publisher:UpdatesPublisher){

    @Operation(summary = "list the current pipelines in the collection", tags = ["Pipeline"],
        description =  """
        There can be zero or more pipelines attache to each service.
        The result contains a service name for each pipeline instance in the pipelines collection. 
    """)
    @GetMapping("/pipeline/list")
    fun list() = pipeline.list()

    @Operation(summary = "register a pipeline for a service", tags = ["Pipeline"])
    @PostMapping("/pipeline")
    suspend fun registerPipeline(@RequestParam("serviceId", defaultValue = "barak") serviceId: String){
        pipeline.addService(serviceId)
    }

    @Operation(summary = "remove a pipeline for a service", tags = ["Pipeline"],
    description =  """
        Remove a pipeline of this service from the pipelines collection, if no such exists ignore silently.
        This method always succeed, to see the status of pipelines collections use "pipelines/list" 
    """)
    @DeleteMapping("/pipeline")
    suspend fun removeService(@RequestParam("serviceId", defaultValue = "barak") serviceId: String){
        pipeline.removePipeline(serviceId)
    }

    @Operation(summary = "produce messages to a service topic", tags = ["Testing"]
    ,description = """
       Will produce to the service topic a sequence of 'count' messages of type Payload
       with the provided retry
    """)
    @PostMapping("/produce")
    fun produce(
        @RequestParam(name="serviceId", defaultValue = "barak") serviceId: String,
        @RequestParam(name="count", defaultValue="1") @Min(1) count: Int,
        @RequestParam(name="retries", defaultValue="0") @Min(0) retries: Int
    ): Flux<String> {
        logger.info("producing: serviceId = $serviceId, count = $count, retries=$retries")
        val messages = Flux.range(0, count).flatMap {
            val payload = Payload(UUID.randomUUID().toString(), retries, 1, "$serviceId $it")
            Flux.just(mapper.writeValueAsString(payload)).onErrorResume { e ->
                logger.error("fail to write payload $payload as string", e)
                Flux.empty()
            }
        }
        return publisher.publish("topic_$serviceId", messages)
    }

    companion object {
        @Suppress("unused")
        val logger: Logger = LoggerFactory.getLogger(ApiEndpoint::class.java)
        val mapper: ObjectMapper = ObjectMapper().registerModule(KotlinModule())

    }
}