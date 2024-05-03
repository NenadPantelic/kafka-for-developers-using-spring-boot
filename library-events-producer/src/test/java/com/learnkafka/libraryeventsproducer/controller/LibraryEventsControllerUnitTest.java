package com.learnkafka.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventsproducer.fixture.LibraryEventFixtureUtil;
import com.learnkafka.libraryeventsproducer.producer.LibraryEventsProducer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

@WebMvcTest(LibraryEventsController.class) // slice part of the application context for these tests - test slice
class LibraryEventsControllerUnitTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    @Autowired
    MockMvc mockMvc; // to inject endpoints

    @MockBean
    LibraryEventsProducer libraryEventsProducer;

    @Test
    void createLibraryEvent() throws Exception {
        // given
        LibraryEvent libraryEvent = LibraryEventFixtureUtil.createLibraryEvent();
        var json = serializeLibraryEvent(libraryEvent);
        Mockito.when(libraryEventsProducer.sendLibraryEventAsProducerRecordWithHeader(
                        Mockito.isA(LibraryEvent.class) // any library event
                ))
                .thenReturn(null);
        // when
        mockMvc.perform(
                        MockMvcRequestBuilders
                                .post("/v1/library-events")
                                .content(json)
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(MockMvcResultMatchers.status().isCreated()

                );

        // then
    }

    @Test
    void createLibraryEventWithInvalidBook() throws Exception {
        // given
        LibraryEvent libraryEvent = LibraryEventFixtureUtil.createLibraryEventWithInvalidBook();
        var json = serializeLibraryEvent(libraryEvent);
        var errMessage = "book.bookAuthor - must not be blank, book.bookId - must not be null, book.bookName - must not be blank";
        Mockito.when(libraryEventsProducer.sendLibraryEventAsProducerRecordWithHeader(
                        Mockito.isA(LibraryEvent.class) // any library event
                ))
                .thenReturn(null);
        // when
        mockMvc.perform(
                        MockMvcRequestBuilders
                                .post("/v1/library-events")
                                .content(json)
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(MockMvcResultMatchers.status().isBadRequest())
                .andExpect(MockMvcResultMatchers.content().string(errMessage));

        // then
    }

    private String serializeLibraryEvent(LibraryEvent libraryEvent) {
        try {
            return OBJECT_MAPPER.writeValueAsString(libraryEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}