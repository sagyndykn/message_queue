package com.nura.messagequeue.repository;

import com.nura.messagequeue.model.document.Topic;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface TopicRepository extends MongoRepository<Topic, String> {

    Optional<Topic> findByName(String name);

    boolean existsByName(String name);

    void deleteByName(String name);
}
