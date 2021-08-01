package com.sapientum8.kafka101

import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

@Configuration
class KafkaConfig {

    @Bean
    fun producerFactory() : ProducerFactory<String,String> =
        DefaultKafkaProducerFactory(
            mapOf(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to "localhost:90920",
                BUFFER_MEMORY_CONFIG to 33554432,
                KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
        ))

    fun kafkaTemplate() : KafkaTemplate<String,String> = KafkaTemplate(producerFactory())

}