package com.learnkafka.libraryeventsconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.libraryeventsconsumer.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public class LibraryEventsRetryConsumer {

    private final LibraryEventService libraryEventService;

    public LibraryEventsRetryConsumer(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    @KafkaListener(
            topics = {"${topics.retry}"},
            autoStartup = "${retryListener.startup:false}", // if the value has not been provided, the default value
            // will be false - it will control whether this listener will be started up by default
            groupId = "retry-listener-group"
    )
    // whenever we have two or more listeners, it isi good to add group id as well
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("ConsumerRecord: {}", consumerRecord);
        consumerRecord.headers().forEach(
                header -> log.info("Key: {}, value: {}", header.key(), new String(header.value()))
        );
        libraryEventService.processLibraryEvent(consumerRecord);
    }
}
