package com.nura.messagequeue.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumeIntegrationTest extends AbstractMongoIntegrationTest {

    @Test
    @DisplayName("messages are consumed in FIFO order")
    void consumeIsFifo() {
        createTopic("fifo", 100);
        for (int i = 1; i <= 5; i++) {
            produce("fifo", "m" + i);
        }

        for (int i = 1; i <= 5; i++) {
            ResponseEntity<String> resp = consume("fifo");
            assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(resp.getBody()).isEqualTo("m" + i);
        }
    }

    @Test
    @DisplayName("consuming an empty queue returns an informative 200")
    void consumeEmptyQueue() {
        createTopic("empty", 100);

        ResponseEntity<String> resp = consume("empty");

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody()).contains("No messages available");
    }

    @Test
    @DisplayName("consuming from a non-existent topic is rejected with 400")
    void consumeMissingTopic() {
        ResponseEntity<String> resp = consume("ghost");

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(resp.getBody()).contains("Topic does not exist");
    }

    @Test
    @DisplayName("consume removes the message so it cannot be read twice")
    void consumeRemovesMessage() {
        createTopic("once", 100);
        produce("once", "only");

        assertThat(consume("once").getBody()).isEqualTo("only");
        assertThat(consume("once").getBody()).contains("No messages available");
        assertThat(queueMessageRepository.countByTopic("once")).isZero();
    }

    @Test
    @DisplayName("concurrent consumers deliver every message exactly once (no duplicates, no loss)")
    void concurrentConsumersDeliverEachMessageExactlyOnce() throws InterruptedException {
        createTopic("race", 10_000);

        int total = 500;
        for (int i = 0; i < total; i++) {
            produce("race", "m" + i);
        }
        assertThat(queueMessageRepository.countByTopic("race")).isEqualTo(total);

        int consumers = 10;
        int attemptsPerConsumer = (total / consumers) + 20;

        ExecutorService pool = Executors.newFixedThreadPool(consumers);
        CountDownLatch startGate = new CountDownLatch(1);
        Set<String> delivered = ConcurrentHashMap.newKeySet();
        List<String> duplicates = Collections.synchronizedList(new ArrayList<>());

        for (int c = 0; c < consumers; c++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < attemptsPerConsumer; i++) {
                    String body = consume("race").getBody();
                    if (body != null && body.startsWith("m")) {
                        if (!delivered.add(body)) {
                            duplicates.add(body);
                        }
                    }
                }
            });
        }

        startGate.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

        assertThat(duplicates)
                .as("no message should be delivered more than once")
                .isEmpty();
        assertThat(delivered)
                .as("every produced message should be delivered exactly once")
                .hasSize(total);
        assertThat(queueMessageRepository.countByTopic("race"))
                .as("queue should be fully drained")
                .isZero();
    }
}
