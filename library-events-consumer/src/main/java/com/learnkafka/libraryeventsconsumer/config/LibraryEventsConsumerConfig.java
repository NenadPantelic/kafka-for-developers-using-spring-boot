package com.learnkafka.libraryeventsconsumer.config;

import com.learnkafka.libraryeventsconsumer.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@Slf4j
//@EnableKafka // to automatically spin up the consumer (needed for older versions of Kafka)
public class LibraryEventsConsumerConfig {

    public static final String RETRY = "RETRY";
    public static final String DEAD = "DEAD";
    public static final String SUCCESS = "SUCCESS";

    private final KafkaProperties properties;
    private final KafkaTemplate kafkaTemplate;

    private FailureService failureService;
    private final String retryTopic;

    private final String deadLetterTopic;

    public LibraryEventsConsumerConfig(KafkaProperties properties,
                                       KafkaTemplate kafkaTemplate,
                                       FailureService failureService,
                                       @Value("${topics.retry}") String retryTopic,
                                       @Value("${topics.dlt}") String deadLetterTopic) {
        this.properties = properties;
        this.kafkaTemplate = kafkaTemplate;
        this.failureService = failureService;
        this.retryTopic = retryTopic;
        this.deadLetterTopic = deadLetterTopic;
    }

    public DeadLetterPublishingRecoverer publishingRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (r, e) -> {
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        return new TopicPartition(retryTopic, r.partition());
                    } else {
                        return new TopicPartition(deadLetterTopic, r.partition());
                    }
                });
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = ((consumerRecord, e) -> {
        log.error("Exception in consumerRecordRecoverer: {}", e.getMessage(), e);
        ConsumerRecord<Integer, String> castedRecord = (ConsumerRecord<Integer, String>) consumerRecord;

        if (e.getCause() instanceof RecoverableDataAccessException) {
            log.info("Recovering from the failure...");
            failureService.saveFailureRecord(castedRecord, e, RETRY);
        } else {
            log.info("Unrecoverable failure happened... ");
            failureService.saveFailureRecord(castedRecord, e, DEAD);
        }
    });

    public DefaultErrorHandler errorHandler() {
        // 1s backoff with 2 retries
        FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2);

        ExponentialBackOffWithMaxRetries exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
        exponentialBackOff.setInitialInterval(1_000L); // will wait for 1s initially
        exponentialBackOff.setMultiplier(2.0); // every time a backoff will be doubled
        exponentialBackOff.setMaxInterval(2_000L); // max interval to wait

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
//                publishingRecoverer(),
//                fixedBackOff,
                consumerRecordRecoverer,
                exponentialBackOff
        );
        // retry is not going to happen upon these exceptions
        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        errorHandler.addRetryableExceptions(RecoverableDataAccessException.class);

        errorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> {
            log.info("Failed record in retry listener. Exception: {}, deliveryAttempt {}", ex.getMessage(), deliveryAttempt);
        }));
        return errorHandler;
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer,
            ObjectProvider<SslBundles> sslBundles) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(
                this.properties.buildConsumerProperties(sslBundles.getIfAvailable()))));
        // manually managing the offset
        // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // set the concurrency level - i.e. the number of consumer threads serving as a consumer
        // so three instances of listener will be brought with their own polling loop
        // practically one consumer app will have multi-threading turned on and e.g. listen to multiple partitions
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        kafkaContainerCustomizer.ifAvailable(factory::setContainerCustomizer);
        return factory;
    }

}
