package com.learnkafka.libraryeventsconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsconsumer.consumer.LibraryEventsConsumer;
import com.learnkafka.libraryeventsconsumer.entity.Book;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEvent;
import com.learnkafka.libraryeventsconsumer.entity.LibraryEventType;
import com.learnkafka.libraryeventsconsumer.repository.FailureRecordRepository;
import com.learnkafka.libraryeventsconsumer.repository.LibraryEventRepository;
import com.learnkafka.libraryeventsconsumer.service.LibraryEventService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
// in-memory, embedded Kafka broker
@EmbeddedKafka(topics = {"library-events", "library-events.RETRY", "library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "retryListener.startup=false" // listener will not be started up by default
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

    @Value("${topics.retry}")
    String retryTopic;
    @Value("${topics.dlt}")
    String deadLetterTopic;

    Consumer<Integer, String> consumer;

    @Autowired
    FailureRecordRepository failureRecordRepository;

    @BeforeEach
    void setUp() {
        // we do not want to wait for all consumers to get their partitions since we disabled retry listener
//        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
//            // all containers/consumers are waiting for partitions to be assigned
//            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
//        }

        // waiting only for the particular consumer
        MessageListenerContainer container = endpointRegistry.getListenerContainers().stream()
                .filter(messageListenerContainer -> "library-events-listener-group".equals(messageListenerContainer.getGroupId()))
                .toList()
                .get(0);

        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
        failureRecordRepository.deleteAll();
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
        // the default backoff retry config is 10

        // then
        // verify the consumer is invoked once
        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(1))
                .onMessage(ArgumentMatchers.isA(ConsumerRecord.class));

        // verify the service is invoked once
        Mockito.verify(libraryEventServiceSpy, Mockito.times(1))
                .processLibraryEvent(ArgumentMatchers.isA(ConsumerRecord.class));

        var configs = new HashMap<>(KafkaTestUtils.consumerProps(
                "group2", // to identify a consumer (its id)
                "true", // autocommit (once the record is ready, flush it)
                embeddedKafkaBroker
        ));
        consumer = new DefaultKafkaConsumerFactory<>(
                configs,
                new IntegerDeserializer(), // key deserializer
                new StringDeserializer() // value deserializer
        ).createConsumer();

        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, deadLetterTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, deadLetterTopic);
        assertThat(consumerRecord.value()).isEqualTo(json);
    }

    @Test
    void publishUpdateLibraryEventWithNonRetryableException() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        // save the new LibraryEvent
        String json = "{\"libraryEventId\":999,\"libraryEventType\":\"UPDATE\"," +
                "\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Nenad\"}}";
        kafkaTemplate.sendDefault(json).get();
        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS); // because Kafka consumer will try to read it 10 times in a row without
        // backoff, so increasing the timeout to be sure there is enough room for that to execute
        // the default backoff retry config is 10

        // then
        // verify the consumer is invoked once
        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(3))
                .onMessage(ArgumentMatchers.isA(ConsumerRecord.class));

        // verify the service is invoked once
        Mockito.verify(libraryEventServiceSpy, Mockito.times(3))
                .processLibraryEvent(ArgumentMatchers.isA(ConsumerRecord.class));

        var configs = new HashMap<>(KafkaTestUtils.consumerProps(
                "group1", // to identify a consumer (its id)
                "true", // autocommit (once the record is ready, flush it)
                embeddedKafkaBroker
        ));
        consumer = new DefaultKafkaConsumerFactory<>(
                configs,
                new IntegerDeserializer(), // key deserializer
                new StringDeserializer() // value deserializer
        ).createConsumer();

        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);
        assertThat(consumerRecord.value()).isEqualTo(json);
    }

    @Test
    void publishUpdateLibraryEventWithFailureRecord() throws JsonProcessingException, ExecutionException, InterruptedException {
        // given
        // save the new LibraryEvent
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\"," +
                "\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Nenad\"}}";
        kafkaTemplate.sendDefault(json).get();
        // when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // then
        // verify the consumer is invoked once
        Mockito.verify(libraryEventsConsumerSpy, Mockito.times(1))
                .onMessage(ArgumentMatchers.isA(ConsumerRecord.class));

        // verify the service is invoked once
        Mockito.verify(libraryEventServiceSpy, Mockito.times(1))
                .processLibraryEvent(ArgumentMatchers.isA(ConsumerRecord.class));

        var count = failureRecordRepository.count();
        assertThat(count).isEqualTo(1);

        failureRecordRepository.findAll()
                .forEach(failureRecord -> assertThat("DEAD").isEqualTo(failureRecord.getStatus()));
    }
}
