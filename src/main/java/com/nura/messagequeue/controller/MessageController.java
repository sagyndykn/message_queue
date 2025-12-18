package com.nura.messagequeue.controller;

import com.nura.messagequeue.model.dto.QueueMessageDTO;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import com.nura.messagequeue.service.TopicService;
import java.util.Optional;

@RestController
@RequestMapping("/queue")
public class MessageController {
    
    private final TopicService topicService;

    public MessageController(TopicService topicService) {
        this.topicService = topicService;
    }

    @PostMapping
    public ResponseEntity<String> postMessage(@RequestBody QueueMessageDTO message) {
        return Optional.of(message)
                .filter(m -> topicService.topicExists(m.getTopic()))
                .filter(m -> topicService.canAcceptMessage(m.getTopic()))
                .map(m -> {
                    topicService.addMessage(m.getTopic(), m.getMessage());
                    return ResponseEntity.ok("Message added to topic '" + m.getTopic() + "': " + m.getMessage());
                })
                .orElseGet(() -> {
                    if (!topicService.topicExists(message.getTopic())) {
                        return ResponseEntity.badRequest().body("Topic does not exist: " + message.getTopic());
                    } else {
                        return ResponseEntity.badRequest().body("Topic queue is full: " + message.getTopic());
                    }
                });
    }

    @GetMapping
    public ResponseEntity<String> getMessage(@RequestParam String topic) {
        if (!topicService.topicExists(topic)) {
            return ResponseEntity.badRequest().body("Topic does not exist: " + topic);
        }
        
        return Optional.ofNullable(topicService.getMessage(topic))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.ok("No messages available for topic: " + topic));
    }
} 
