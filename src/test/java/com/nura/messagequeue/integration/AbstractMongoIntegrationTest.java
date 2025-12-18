package com.nura.messagequeue.integration;

import com.nura.messagequeue.model.dto.TopicDTO;
import com.nura.messagequeue.repository.QueueMessageRepository;
import com.nura.messagequeue.repository.TopicRepository;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public abstract class AbstractMongoIntegrationTest {

    private static final String DEFAULT_URI = "mongodb://localhost:27017/message_queue_it";

    private static String mongoUri() {
        String uri = System.getProperty("test.mongodb.uri");
        if (uri == null || uri.isBlank()) {
            uri = System.getenv("TEST_MONGODB_URI");
        }
        return (uri == null || uri.isBlank()) ? DEFAULT_URI : uri;
    }

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", AbstractMongoIntegrationTest::mongoUri);
        registry.add("spring.data.mongodb.auto-index-creation", () -> "true");
    }

    @LocalServerPort
    protected int port;

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected TopicRepository topicRepository;

    @Autowired
    protected QueueMessageRepository queueMessageRepository;

    @BeforeEach
    void cleanDatabase() {
        queueMessageRepository.deleteAll();
        topicRepository.deleteAll();
    }

    protected String url(String path) {
        return "http://localhost:" + port + path;
    }

    protected void createTopic(String name, int maxSize) {
        TopicDTO topic = new TopicDTO(name, 1, maxSize, "text");
        ResponseEntity<String> resp = rest.postForEntity(url("/topics"), topic, String.class);
        if (!resp.getStatusCode().is2xxSuccessful()) {
            throw new IllegalStateException("Failed to create topic: " + resp.getBody());
        }
    }

    protected ResponseEntity<String> produce(String topic, String message) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String body = "{\"topic\":\"" + topic + "\",\"message\":\"" + message + "\"}";
        return rest.postForEntity(url("/queue"), new HttpEntity<>(body, headers), String.class);
    }

    protected ResponseEntity<String> consume(String topic) {
        return rest.getForEntity(url("/queue?topic=" + topic), String.class);
    }
}
