package com.learnkafka.libraryeventsproducer.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.stream.Collectors;

@ControllerAdvice
@Slf4j
public class LibraryEventsControllerAdvice {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<?> handleException(MethodArgumentNotValidException e) {
        String errMessage = e.getBindingResult().getFieldErrors()
                .stream()
                .map(fieldError -> String.format("%s - %s", fieldError.getField(), fieldError.getDefaultMessage()))
                .sorted()
                .collect(Collectors.joining(", "));
        log.warn("Error: {}", errMessage);
        return new ResponseEntity<>(errMessage, HttpStatus.BAD_REQUEST);
    }
}
