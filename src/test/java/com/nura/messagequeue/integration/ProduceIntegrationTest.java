package com.nura.messagequeue.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ProduceIntegrationTest extends AbstractMongoIntegrationTest {

    @Test
    @DisplayName("produce to an existing topic stores the message and returns 200")
    void produceStoresMessage() {
        createTopic("orders", 100);

        ResponseEntity<String> resp = produce("orders", "hello");

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody()).contains("Message added to topic 'orders'");
        assertThat(queueMessageRepository.countByTopic("orders")).isEqualTo(1);
    }

    @Test
    @DisplayName("produce to a non-existent topic is rejected with 400")
    void produceToMissingTopicRejected() {
        ResponseEntity<String> resp = produce("ghost", "hello");

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(resp.getBody()).contains("Topic does not exist");
        assertThat(queueMessageRepository.countByTopic("ghost")).isZero();
    }

    @Test
    @DisplayName("produce is rejected once the queue reaches maxSize")
    void produceRespectsMaxSize() {
        createTopic("small", 3);

        for (int i = 1; i <= 3; i++) {
            assertThat(produce("small", "m" + i).getStatusCode()).isEqualTo(HttpStatus.OK);
        }

        ResponseEntity<String> overflow = produce("small", "m4");
        assertThat(overflow.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(overflow.getBody()).contains("Topic queue is full");
        assertThat(queueMessageRepository.countByTopic("small")).isEqualTo(3);
    }

    @Test
    @DisplayName("many messages produced sequentially are all persisted")
    void produceManySequential() {
        createTopic("bulk", 10_000);

        int n = 500;
        for (int i = 0; i < n; i++) {
            assertThat(produce("bulk", "m" + i).getStatusCode()).isEqualTo(HttpStatus.OK);
        }

        assertThat(queueMessageRepository.countByTopic("bulk")).isEqualTo(n);
    }

    @Test
    @DisplayName("concurrent producers all succeed with no lost writes")
    void concurrentProducersNoLostWrites() throws InterruptedException {
        createTopic("concurrent", 10_000);

        int producers = 8;
        int perProducer = 100;
        int total = producers * perProducer;
        ExecutorService pool = Executors.newFixedThreadPool(producers);
        AtomicInteger ok = new AtomicInteger();

        for (int p = 0; p < producers; p++) {
            final int producerId = p;
            pool.submit(() -> {
                for (int i = 0; i < perProducer; i++) {
                    ResponseEntity<String> resp = produce("concurrent", "p" + producerId + "-m" + i);
                    if (resp.getStatusCode() == HttpStatus.OK) {
                        ok.incrementAndGet();
                    }
                }
            });
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();

        assertThat(ok.get()).isEqualTo(total);
        assertThat(queueMessageRepository.countByTopic("concurrent")).isEqualTo(total);
    }
}
