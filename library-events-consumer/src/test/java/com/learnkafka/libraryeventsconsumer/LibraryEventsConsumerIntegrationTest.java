package com.learnkafka.libraryeventsconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsconsumer.consumer.LibraryEventsConsumer;
import com.learnkafka.libraryeventsconsumer.entity.Book;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEvent;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEventType;
import com.learnkafka.libraryeventsconsumer.repository.LibraryEventRepository;
import com.learnkafka.libraryeventsconsumer.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(topics = "library-events", partitions = 3) // in-memory, embedded Kafka broker
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    // mock a producer
    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean // spy bean is actually a spy that gives access to an actual bean/object
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventService libraryEventServiceSpy;

    @Autowired
    LibraryEventRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
            // all containers/consumers are waiting for partitions to be assigned
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        // given
        String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456," +
                "\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Nenad\"}}";
        int bookId = 456;
        kafkaTemplate.sendDefault(json).get(); // sending a message to default topic

        // when
        CountDownLatch latch = new CountDownLatch(1); // block a thread for 3 seconds, allowing a produce-consume cycle
        // to happen
        latch.await(3, TimeUnit.SECONDS);

        // then
        // verify the consumer is invoked once
        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(1))
                .onMessage(ArgumentMatchers.isA(ConsumerRecord.class));

        // verify the service is invoked once
        Mockito.verify(libraryEventServiceSpy, Mockito.times(1))
                .processLibraryEvent(ArgumentMatchers.isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEvents = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assertThat(libraryEvents.size()).isEqualTo(1);

        libraryEvents.forEach(libraryEvent -> {
            assertThat(libraryEvent.getLibraryEventId()).isNotNull();
            assertThat(libraryEvent.getBook().getBookId()).isEqualTo(bookId);
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        // save the new LibraryEvent
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\"," +
                "\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Nenad\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        int bookId = 456;
        String newBookAuthor = "Nenad P";
        String newBookName = "Kafka Using Spring Boot 2nd Edition";

        // publish the update LibraryEvent
        Book updatedBook = Book.builder()
                .bookId(bookId)
                .bookAuthor(newBookAuthor)
                .bookName(newBookName)
                .build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);

        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent)).get();

        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        // then
        // verify the consumer is invoked once
        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(1))
                .onMessage(ArgumentMatchers.isA(ConsumerRecord.class));

        // verify the service is invoked once
        Mockito.verify(libraryEventServiceSpy, Mockito.times(1))
                .processLibraryEvent(ArgumentMatchers.isA(ConsumerRecord.class));


        LibraryEvent libraryEventAfterUpdate = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
        assertThat(libraryEventAfterUpdate.getLibraryEventType()).isEqualTo(LibraryEventType.UPDATE);

        Book actualBook = libraryEvent.getBook();
        assertThat(actualBook.getBookId()).isEqualTo(bookId);
        assertThat(actualBook.getBookAuthor()).isEqualTo(newBookAuthor);
        assertThat(actualBook.getBookName()).isEqualTo(newBookName);
    }

    @Test
    void publishUpdateLibraryEventWithNullEventId() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        // save the new LibraryEvent
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\"," +
                "\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Nenad\"}}";
        kafkaTemplate.sendDefault(json).get();
        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS); // because Kafka consumer will try to read it 10 times in a row without
        // backoff, so increasing the timeout to be sure there is enough room for that to execute

        // then
        // verify the consumer is invoked once
        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(10))
                .onMessage(ArgumentMatchers.isA(ConsumerRecord.class));

        // verify the service is invoked once
        Mockito.verify(libraryEventServiceSpy, Mockito.times(10))
                .processLibraryEvent(ArgumentMatchers.isA(ConsumerRecord.class));
    }
}
