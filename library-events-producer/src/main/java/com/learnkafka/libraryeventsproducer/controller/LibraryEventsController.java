package com.learnkafka.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventsproducer.producer.LibraryEventsProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Slf4j
@RestController
public class LibraryEventsController {

    private final LibraryEventsProducer libraryEventsProducer;

    public LibraryEventsController(LibraryEventsProducer libraryEventsProducer) {
        this.libraryEventsProducer = libraryEventsProducer;
    }

    @PostMapping("/v1/library-events")
    public ResponseEntity<LibraryEvent> createLibraryEvent(@Valid @RequestBody LibraryEvent libraryEvent) throws JsonProcessingException
            //{
            , ExecutionException, InterruptedException, TimeoutException {
        log.info("Creating a library event: {}", libraryEvent);
//        libraryEventsProducer.sendLibraryEvent(libraryEvent);
//        libraryEventsProducer.sendLibraryEventSync(libraryEvent);
//        libraryEventsProducer.sendLibraryEventAsProducerRecord(libraryEvent);
        libraryEventsProducer.sendLibraryEventAsProducerRecordWithHeader(libraryEvent);
        log.info("After sending a library event message");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
