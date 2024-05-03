package com.learnkafka.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventsproducer.fixture.LibraryEventFixtureUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;

// brings up the whole Spring app with context
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT) // every time runs on a random port
@EmbeddedKafka(topics = "library-events", partitions = 3) // in-memory, embedded Kafka broker
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"}
)
class LibraryEventsControllerIntegrationTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    TestRestTemplate restTemplate; // the port will be pre-set to random port

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        // embeds properties into a broker
        var configs = new HashMap<>(KafkaTestUtils.consumerProps(
                "group1", // to identify a consumer (its id)
                "true", // autocommit (once the record is ready, flush it
                embeddedKafkaBroker
        ));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // read only latest messages
        consumer = new DefaultKafkaConsumerFactory<>(
                configs,
                new IntegerDeserializer(), // key deserializer
                new StringDeserializer() // value deserializer
        ).createConsumer();

        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void createLibraryEvent() {
        // given
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON_VALUE);

        LibraryEvent libraryEvent = LibraryEventFixtureUtil.createLibraryEvent();
        var httpEntity = new HttpEntity<>(
                libraryEvent,
                httpHeaders
        );

        // when
        ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange(
                "/v1/library-events",
                HttpMethod.POST,
                httpEntity,
                LibraryEvent.class
        );

        // then
        Assertions.assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        // blocking call to get all consumer records
        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);
        Assertions.assertThat(consumerRecords.count()).isEqualTo(1);

        consumerRecords.forEach(record -> {
                    LibraryEvent actual = parseLibraryEventRecord(record.value());
                    Assertions.assertThat(actual).isEqualTo(libraryEvent);
                }
        );
    }

    private LibraryEvent parseLibraryEventRecord(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, LibraryEvent.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}