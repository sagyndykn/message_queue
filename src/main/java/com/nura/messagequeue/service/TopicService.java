package com.nura.messagequeue.service;

import com.nura.messagequeue.model.document.QueueMessage;
import com.nura.messagequeue.model.document.Topic;
import com.nura.messagequeue.model.dto.TopicDTO;
import com.nura.messagequeue.repository.QueueMessageRepository;
import com.nura.messagequeue.repository.TopicRepository;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class TopicService {
    private final QueueMessageRepository queueMessageRepository;
    private final TopicRepository topicRepository;
    private final MongoTemplate mongoTemplate;

    public TopicService(
            QueueMessageRepository queueMessageRepository,
            TopicRepository topicRepository,
            MongoTemplate mongoTemplate
    ) {
        this.queueMessageRepository = queueMessageRepository;
        this.topicRepository = topicRepository;
        this.mongoTemplate = mongoTemplate;
    }

    public void createTopic(TopicDTO topicDto) {
        Topic topic = new Topic(
            topicDto.getName(),
            topicDto.getInstances(),
            topicDto.getMaxSize(),
            topicDto.getMessageFormat()
        );
        topicRepository.save(topic);
    }

    public boolean topicExists(String topicName) {
        return topicRepository.existsByName(topicName);
    }

    public boolean canAcceptMessage(String topicName) {
        Optional<Topic> topicOpt = topicRepository.findByName(topicName);
        if (topicOpt.isEmpty()) {
            return false;
        }

        long currentMessageCount = queueMessageRepository.countByTopic(topicName);
        return currentMessageCount < topicOpt.get().getMaxSize();
    }

    public void addMessage(String topicName, String message) {
        if (topicExists(topicName)) {
            QueueMessage QueueMessage = new QueueMessage(topicName, message);
            queueMessageRepository.save(QueueMessage);
        }
    }

    public String getMessage(String topicName) {
        if (!topicExists(topicName)) {
            return null;
        }
        
        Query query = Query.query(Criteria.where("topic").is(topicName))
                .with(Sort.by(Sort.Direction.ASC, "createdAt", "_id"))
                .limit(1);

        QueueMessage message = mongoTemplate.findAndRemove(query, QueueMessage.class);

        if (message != null) {
            return message.getMessage();
        }
        
        return null;
    }

    public TopicDTO getTopicConfig(String topicName) {
        Optional<Topic> topicOpt = topicRepository.findByName(topicName);
        if (topicOpt.isPresent()) {
            Topic Topic = topicOpt.get();
            return new TopicDTO(
                    Topic.getName(),
                    Topic.getInstances(),
                    Topic.getMaxSize(),
                    Topic.getMessageFormat()
            );
        }
        return null;
    }

    public Map<String, TopicDTO> getAllTopics() {
        List<Topic> topics = topicRepository.findAll();
        Map<String, TopicDTO> result = new HashMap<>();
        
        for (Topic topic : topics) {
            TopicDTO topicDto = new TopicDTO(
                    topic.getName(),
                    topic.getInstances(),
                    topic.getMaxSize(),
                    topic.getMessageFormat()
            );
            result.put(topic.getName(), topicDto);
        }
        
        return result;
    }
} 
