package com.learnkafka.libraryeventsconsumer.service;

import com.learnkafka.libraryeventsconsumer.entity.FailureRecord;
import com.learnkafka.libraryeventsconsumer.repository.FailureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FailureService {

    private final FailureRecordRepository failureRecordRepository;

    public FailureService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailureRecord(ConsumerRecord<Integer, String> consumerRecord, Exception e, String status) {
        log.info("Saving failure record: {}", consumerRecord);

        FailureRecord failureRecord = new FailureRecord(
                null,
                consumerRecord.topic(),
                consumerRecord.key(),
                consumerRecord.value(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                e.getCause() != null ? e.getCause().getMessage() : "N/A",
                status
        );

        failureRecordRepository.save(failureRecord);
    }
}

