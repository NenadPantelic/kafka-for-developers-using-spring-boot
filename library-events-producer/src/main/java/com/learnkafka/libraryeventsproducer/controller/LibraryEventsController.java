package com.learnkafka.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventsproducer.domain.LibraryEventType;
import com.learnkafka.libraryeventsproducer.producer.LibraryEventsProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
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

    @PutMapping("/v1/library-events")
    public ResponseEntity<?> updateLibraryEvent(@Valid @RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("Creating a library event: {}", libraryEvent);

        ResponseEntity<String> validationResult = validateLibraryEvent(libraryEvent);
        if (validationResult != null) {
            return validationResult;
        }

        libraryEventsProducer.sendLibraryEventAsProducerRecordWithHeader(libraryEvent);
        log.info("After sending a library event message");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    private static ResponseEntity<String> validateLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.libraryEventId() == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the library event id");
        }

        if (libraryEvent.libraryEventType() != LibraryEventType.UPDATE) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Invalid library event type");
        }

        return null;
    }
}
