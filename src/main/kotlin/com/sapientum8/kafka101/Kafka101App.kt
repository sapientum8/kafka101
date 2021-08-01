package com.sapientum8.kafka101

import com.github.javafaker.Faker
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationStartedEvent
import org.springframework.boot.runApplication
import org.springframework.context.event.EventListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.stream.Stream

@SpringBootApplication
class Kafka101App

@Component
class Producer(val template: KafkaTemplate<Int,String>) {
    val faker: Faker = Faker.instance()

    @EventListener(ApplicationStartedEvent::class)
    fun generate() {
        val interval = Flux.interval(Duration.ofMillis(1_000))

        val quotes = Flux.fromStream(Stream.generate(faker.hobbit()::quote))

        Flux.zip(interval,quotes)
            .map { it -> template.send("hobbit",faker.random().nextInt(101),it.t2) }
            .blockLast()
    }
}

fun main(args: Array<String>) {
    runApplication<Kafka101App>(*args)
}
