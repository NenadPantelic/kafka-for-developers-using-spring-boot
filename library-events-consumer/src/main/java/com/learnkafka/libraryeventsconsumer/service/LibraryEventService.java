package com.learnkafka.libraryeventsconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEvent;
import com.learnkafka.libraryeventsconsumer.repository.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class LibraryEventService {

    private final ObjectMapper objectMapper;

    private final LibraryEventRepository libraryEventRepository;

    public LibraryEventService(ObjectMapper objectMapper, LibraryEventRepository libraryEventRepository) {
        this.objectMapper = objectMapper;
        this.libraryEventRepository = libraryEventRepository;
    }


    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        log.info("Processing library event {}", consumerRecord);

        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        if (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 999) {
            throw new RecoverableDataAccessException("Temporary network issue");
        }

        switch (libraryEvent.getLibraryEventType()) {
            case NEW -> {
                saveLibraryEvent(libraryEvent);
                log.info("Successfully persisted the library event {}", libraryEvent);
            }

            case UPDATE -> {
                validate(libraryEvent);
                saveLibraryEvent(libraryEvent);
                log.info("Successfully updated the library event {}", libraryEvent);
            }

            default -> throw new IllegalStateException("Unexpected value: " + libraryEvent.getLibraryEventType());
        }
    }

    private void saveLibraryEvent(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event id is missing");
        }

        libraryEventRepository.findById(libraryEvent.getLibraryEventId())
                .orElseThrow(() -> new IllegalArgumentException("Not a valid library event"));

        log.info("Library event validation successfully passed");
    }
}
