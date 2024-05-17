package com.learnkafka.libraryeventsconsumer.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class FailureRecord {

    @Id
    @GeneratedValue
    private Integer id;
    private String topic;
    private Integer keyValue;
    private String errorRecord;
    private Integer partition;
    private Long offsetValue;
    private String exception;
    private String status;
}
