package com.learnkafka.libraryeventsconsumer.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.libraryeventsconsumer.config.LibraryEventsConsumerConfig;
import com.learnkafka.libraryeventsconsumer.entity.FailureRecord;
import com.learnkafka.libraryeventsconsumer.repository.FailureRecordRepository;
import com.learnkafka.libraryeventsconsumer.service.LibraryEventService;
import jakarta.persistence.criteria.CriteriaBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class RetryScheduler {

    private final FailureRecordRepository failureRecordRepository;
    private final LibraryEventService libraryEventService;

    public RetryScheduler(FailureRecordRepository failureRecordRepository,
                          LibraryEventService libraryEventService) {
        this.failureRecordRepository = failureRecordRepository;
        this.libraryEventService = libraryEventService;
    }

    @Scheduled(fixedRate = 10000)
    public void retryConsumingFailedRecords() {
        log.info("Retry consuming a failed record...");
        List<FailureRecord> failureRecords = failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY);
        failureRecords.forEach(failureRecord -> {
            log.info("Retrying failied record: {}", failureRecord);
            try {
                libraryEventService.processLibraryEvent(buildConsumerRecord(failureRecord));
                failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
                failureRecordRepository.save(failureRecord);
            } catch (Exception e) {
                log.error("Exception in retry consuming failed record: {}", e.getMessage(), e);
            }
        });
    }


    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(
                failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffsetValue(),
                failureRecord.getKeyValue(),
                failureRecord.getErrorRecord()
        );
    }
}
