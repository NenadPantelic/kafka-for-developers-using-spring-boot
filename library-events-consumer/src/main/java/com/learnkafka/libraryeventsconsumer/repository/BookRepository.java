package com.learnkafka.libraryeventsconsumer.repository;


import com.learnkafka.libraryeventsconsumer.entity.Book;
import org.springframework.data.repository.CrudRepository;

public interface BookRepository extends CrudRepository<Book, Integer> {
}
