package com.nura.messagequeue.controller;

import com.nura.messagequeue.model.dto.TopicDTO;
import com.nura.messagequeue.service.TopicService;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/topics")
public class TopicController {
    
    private final TopicService topicService;

    public TopicController(TopicService topicService) {
        this.topicService = topicService;
    }

    @PostMapping
    public ResponseEntity<String> createTopic(@RequestBody TopicDTO topicDto) {
        if (topicService.topicExists(topicDto.getName())) {
            return ResponseEntity.badRequest().body("Topic already exists: " + topicDto.getName());
        }
        topicService.createTopic(topicDto);
        return ResponseEntity.ok("Topic created: " + topicDto.getName());
    }

    @GetMapping
    public ResponseEntity<Map<String, TopicDTO>> getAllTopics() {
        return ResponseEntity.ok(topicService.getAllTopics());
    }

    @GetMapping("/{name}")
    public ResponseEntity<TopicDTO> getTopic(@PathVariable String name) {
        return Optional.ofNullable(topicService.getTopicConfig(name))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
} 
