package com.nura.messagequeue.repository;

import com.nura.messagequeue.model.document.QueueMessage;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface QueueMessageRepository extends MongoRepository<QueueMessage, String> {

    Optional<QueueMessage> findFirstByTopicOrderByCreatedAtDesc(String topic);

    Optional<QueueMessage> findFirstByTopicOrderByCreatedAtAsc(String topic);

    List<QueueMessage> findAllByTopicOrderByCreatedAtAsc(String topic);

    long countByTopic(String topic);
}
