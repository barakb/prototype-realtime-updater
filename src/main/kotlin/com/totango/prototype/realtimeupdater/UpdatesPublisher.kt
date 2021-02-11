package com.totango.prototype.realtimeupdater

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import java.io.Closeable
import java.text.SimpleDateFormat
import java.util.*


@Component
class UpdatesPublisher(properties: RealtimeUpdaterProperties) : Closeable {
    private val dateFormat = SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy")
    private val props = properties.kafka.producer.build()

    private val senderOptions: SenderOptions<Int, String> = SenderOptions.create(props)
    private val sender: KafkaSender<Int, String> = KafkaSender.create(senderOptions)


    fun publish(topic: String, messages: Flux<String>): Flux<String> {
        val records = messages.map {
            SenderRecord.create(ProducerRecord(topic, 1, it), it)
        }
        return publishRecords(records)
    }

    private fun publishRecords(messages: Flux<SenderRecord<Int, String, String>>) =
        sender.send(messages).map {
            val metadata: RecordMetadata = it.recordMetadata()
            "Message ${it.correlationMetadata()} sent successfully," +
                    " topic-partition=${metadata.topic()}-${metadata.partition()}" +
                    " offset=${metadata.offset()}" +
                    " timestamp=${dateFormat.format(Date(metadata.timestamp()))}\n"
        }

    override fun close() {
        sender.close()
    }

    companion object {
        @Suppress("unused")
        val logger: Logger = LoggerFactory.getLogger(UpdatesPublisher::class.java)
    }

}