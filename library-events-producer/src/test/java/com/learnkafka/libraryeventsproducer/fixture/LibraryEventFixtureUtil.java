package com.learnkafka.libraryeventsproducer.fixture;

import com.learnkafka.libraryeventsproducer.domain.Book;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventsproducer.domain.LibraryEventType;

public class LibraryEventFixtureUtil {

    public static LibraryEvent createLibraryEvent() {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Nenad")
                .bookName("Kafka for Spring developers")
                .build();

        return LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();
    }

    public static LibraryEvent createLibraryEventWithInvalidBook() {
        Book book = Book.builder()
                .bookId(null)
                .bookAuthor("")
                .bookName("")
                .build();

        return LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();
    }
}
