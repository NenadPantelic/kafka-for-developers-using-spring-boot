package com.learnkafka.libraryeventsproducer.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;

@Builder
public record Book(@NotNull Integer bookId,
                   @NotBlank String bookName,
                   @NotBlank String bookAuthor) {
}
