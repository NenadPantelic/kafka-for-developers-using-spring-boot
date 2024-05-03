package com.learnkafka.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class LibraryEventsProducer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    @Value("${spring.kafka.topic}")
    public String topic;

    public LibraryEventsProducer(KafkaTemplate<Integer, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    // approach 1 - asynchronous sending...
    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(
            LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.libraryEventId();
        String value = OBJECT_MAPPER.writeValueAsString(libraryEvent);

        // 1. blocking call - get metadata about the Kafka cluster (just for the first time the code is triggered)
        // 2. Send message - returns a completable future (every time)
        CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(topic, key, value);

        return completableFuture.whenComplete((sendResult, throwable) -> {
            if (throwable != null) {
                handleFailure(key, value, throwable);
            } else {
                handleSuccess(key, value, sendResult);
            }
        });
    }

    // approach 2 - synchronous sending...
    public SendResult<Integer, String> sendLibraryEventSync(
            LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        Integer key = libraryEvent.libraryEventId();
        String value = OBJECT_MAPPER.writeValueAsString(libraryEvent);

        // 1. blocking call - get metadata about the Kafka cluster (just for the first time the code is triggered)
        // 2. Block and wait until the message is sent to Kafka
        SendResult<Integer, String> sendResult = kafkaTemplate.send(topic, key, value)
//                .get();
                .get(3, TimeUnit.SECONDS);
        handleSuccess(key, value, sendResult);
        return sendResult;
    }

    // approach 3 - sending asynchronously as producer record
    public CompletableFuture<SendResult<Integer, String>> sendLibraryEventAsProducerRecord(
            LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.libraryEventId();
        String value = OBJECT_MAPPER.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value);
        // 1. blocking call - get metadata about the Kafka cluster (just for the first time the code is triggered)
        // 2. Send message as producer record - returns a completable future (every time)
        CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(producerRecord);

        return completableFuture.whenComplete((sendResult, throwable) -> {
            if (throwable != null) {
                handleFailure(key, value, throwable);
            } else {
                handleSuccess(key, value, sendResult);
            }
        });
    }

    // approach 4 - sending asynchronously as producer record with header
    public CompletableFuture<SendResult<Integer, String>> sendLibraryEventAsProducerRecordWithHeader(
            LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.libraryEventId();
        String value = OBJECT_MAPPER.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProducerRecordWithHeader(key, value);
        // 1. blocking call - get metadata about the Kafka cluster (just for the first time the code is triggered)
        // 2. Send message as producer record with header - returns a completable future (every time)
        CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(producerRecord);

        return completableFuture.whenComplete((sendResult, throwable) -> {
            if (throwable != null) {
                handleFailure(key, value, throwable);
            } else {
                handleSuccess(key, value, sendResult);
            }
        });
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
        log.info("Building a producer record: key = {}, value = {}", key, value);
        return new ProducerRecord<>(topic, key, value);
    }

    private ProducerRecord<Integer, String> buildProducerRecordWithHeader(Integer key, String value) {
        log.info("Building a producer record: key = {}, value = {}", key, value);
        List<Header> headers = List.of(
                new RecordHeader("event-source", "scanner".getBytes())
        );
        return new ProducerRecord<>(topic, null, key, value, headers);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info(
                "Message sent successfully for: key = {}, value = {}, partition = {}",
                key, value, sendResult.getRecordMetadata().partition()
        );
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error(
                "An error occurred when sending the message {}/{} due to {}",
                key, value, throwable.getMessage(), throwable
        );

    }
}
