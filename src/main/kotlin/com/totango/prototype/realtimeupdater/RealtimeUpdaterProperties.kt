package com.totango.prototype.realtimeupdater

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.PropertyMapper
import org.springframework.stereotype.Component


fun <K,V> MutableMap<K,V>.into(key: K) = { value:V -> this[key] = value}

@Suppress("RemoveEmptyPrimaryConstructor", "MemberVisibilityCanBePrivate")
@ConfigurationProperties
class ConsumerProperties(){
    lateinit var bootstrapServers: String
    lateinit var groupId: String
    var autoOffsetReset: String? = null
    var keyDeserializer : Class<*> = StringDeserializer::class.java
    var valueDeserializer : Class<*> = StringDeserializer::class.java

    fun build(): Map<String, Any> {
        val properties: MutableMap<String, Any> = mutableMapOf()
        val propertyMapper = PropertyMapper.get().alwaysApplyingWhenNonNull()
        propertyMapper.from(bootstrapServers).to(properties.into(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG))
        propertyMapper.from(groupId).to(properties.into(ConsumerConfig.GROUP_ID_CONFIG))
        propertyMapper.from(keyDeserializer).to(properties.into(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
        propertyMapper.from(valueDeserializer).to(properties.into(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG))
        autoOffsetReset?.let {propertyMapper.from(it).to(properties.into(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG))}
        return properties
    }
}
@Suppress("RemoveEmptyPrimaryConstructor", "MemberVisibilityCanBePrivate")
@ConfigurationProperties
class ProducerProperties(){
    lateinit var bootstrapServers: String
    var acks: String? = null
    var keySerializer : Class<*> = StringSerializer::class.java
    var valueSerializer : Class<*> = StringSerializer::class.java

    fun build(): Map<String, Any> {
        val properties: MutableMap<String, Any> = mutableMapOf()
        val propertyMapper = PropertyMapper.get().alwaysApplyingWhenNonNull()
        propertyMapper.from(bootstrapServers).to(properties.into(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
        propertyMapper.from(keySerializer).to(properties.into(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
        propertyMapper.from(valueSerializer).to(properties.into(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
        acks?.let{propertyMapper.from(it).to(properties.into(ProducerConfig.ACKS_CONFIG))}
        return properties
    }
}

@ConfigurationProperties
data class Kafka(
    var consumer: ConsumerProperties = ConsumerProperties(),
    var producer: ProducerProperties = ProducerProperties()
)

@Component
@ConfigurationProperties("settings")
data class RealtimeUpdaterProperties(
    val kafka : Kafka = Kafka(),
    val batchSize: Int = 10,
    val batchMaxDelay: Long = 10 * 1000,
    val batchRetries: Int = 3,
    val subscriberThreads: Int = 1,
    val updaterThreads: Int = 5,
    val inFlightUpdates: Int = 20,
    val retryDelay: Int = 5,
)



