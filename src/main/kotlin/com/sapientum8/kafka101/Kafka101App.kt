package com.sapientum8.kafka101

import com.github.javafaker.Faker
import lombok.RequiredArgsConstructor
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationStartedEvent
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.event.EventListener
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import java.time.Duration
import java.util.stream.Stream


@Configuration
@EnableKafkaStreams
@SpringBootApplication
class Kafka101App {
    @Bean
    fun counts():NewTopic? = TopicBuilder.name("streams-wordcount-output").partitions(6).replicas(3).build()
}

@Component
class Producer(val template:KafkaTemplate<Int,String>) {
    val faker:Faker = Faker.instance()

    @EventListener(ApplicationStartedEvent::class)
    fun generate() {
        val interval = Flux.interval(Duration.ofMillis(1_000))

        val quotes = Flux.fromStream(Stream.generate(faker.hobbit()::quote))

        Flux.zip(interval,quotes)
            .map {it -> template.send("hobbit",faker.random().nextInt(101),it.t2)}
            .blockLast()
    }
}

@Component
class Consumer {
    @KafkaListener(topics = ["streams-wordcount-output"],groupId = "spring-boot-kafka")
    fun consume(record:ConsumerRecord<String,Long>) {
        println("received = ${record.value()} with key ${record.key()}")
    }
}

@Component
class Processor {
    @Autowired
    fun process(builder:StreamsBuilder) {
        val integerSerde = Serdes.Integer()
        val stringSerde = Serdes.String()
        val longSerde = Serdes.Long()

        val textLines:KStream<Int,String> = builder.stream("hobbit",Consumed.with(integerSerde,stringSerde))

        val wordCounts:KTable<String,Long> = textLines
            .flatMapValues(ValueMapper {it.lowercase().split(" ")})
            .groupBy({_,v -> v},Grouped.with(stringSerde,stringSerde))
            .count(Materialized.`as`("counts"))

        wordCounts.toStream().to("streams-wordcount-output",Produced.with(stringSerde,longSerde))
    }
}

@RestController
class RestService(private val factoryBean:StreamsBuilderFactoryBean) {
    @GetMapping("/count/{word}")
    fun getCount(@PathVariable word:String) : Long {
        val kafkaStreams = factoryBean.kafkaStreams
        val counts:ReadOnlyKeyValueStore<String,Long> = kafkaStreams.store(StoreQueryParameters.fromNameAndType("counts",QueryableStoreTypes.keyValueStore()))
        return counts.get(word)
    }
}

fun main(args:Array<String>) {
    runApplication<Kafka101App>(*args)
}
