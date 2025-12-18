package com.nura.messagequeue.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ProductionLoadIntegrationTest extends AbstractMongoIntegrationTest {

    private final AtomicInteger serverErrors = new AtomicInteger();

    @Test
    @DisplayName("producers and consumers work one topic simultaneously: no loss, no duplicates, no 5xx")
    void concurrentProduceAndConsumeSingleTopic() throws InterruptedException {
        String topic = "broker";
        int producers = 8;
        int perProducer = 500;
        int total = producers * perProducer;
        int consumers = 12;
        createTopic(topic, total + 1000);

        Set<String> delivered = ConcurrentHashMap.newKeySet();
        CopyOnWriteArrayList<String> duplicates = new CopyOnWriteArrayList<>();
        AtomicBoolean producersDone = new AtomicBoolean(false);
        AtomicInteger producedOk = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(producers + consumers);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch producersFinished = new CountDownLatch(producers);

        for (int p = 0; p < producers; p++) {
            final int producerId = p;
            pool.submit(() -> {
                await(startGate);
                try {
                    for (int i = 0; i < perProducer; i++) {
                        ResponseEntity<String> resp = produce(topic, "p" + producerId + "-m" + i);
                        if (resp.getStatusCode() == HttpStatus.OK) {
                            producedOk.incrementAndGet();
                        } else {
                            countIfServerError(resp);
                        }
                    }
                } finally {
                    producersFinished.countDown();
                }
            });
        }

        long deadline = System.currentTimeMillis() + 120_000;
        for (int c = 0; c < consumers; c++) {
            pool.submit(() -> {
                await(startGate);
                while (System.currentTimeMillis() < deadline) {
                    if (producersDone.get() && delivered.size() >= producedOk.get()) {
                        break;
                    }
                    ResponseEntity<String> resp = consume(topic);
                    if (resp.getStatusCode() != HttpStatus.OK) {
                        countIfServerError(resp);
                        continue;
                    }
                    String body = resp.getBody();
                    if (body != null && body.startsWith("p")) {
                        if (!delivered.add(body)) {
                            duplicates.add(body);
                        }
                    }
                }
            });
        }

        startGate.countDown();
        assertThat(producersFinished.await(120, TimeUnit.SECONDS))
                .as("producers should finish in time").isTrue();
        producersDone.set(true);

        pool.shutdown();
        assertThat(pool.awaitTermination(120, TimeUnit.SECONDS))
                .as("consumers should drain the queue in time").isTrue();

        assertThat(producedOk.get()).as("all messages accepted").isEqualTo(total);
        assertThat(serverErrors.get()).as("no 5xx responses under load").isZero();
        assertThat(duplicates).as("no message delivered more than once").isEmpty();
        assertThat(delivered).as("every produced message delivered exactly once").hasSize(total);
        assertThat(queueMessageRepository.countByTopic(topic)).as("queue fully drained").isZero();
    }

    @Test
    @DisplayName("many topics under concurrent produce/consume stay isolated and lossless")
    void concurrentProduceAndConsumeManyTopics() throws InterruptedException {
        int topicCount = 5;
        int producersPerTopic = 4;
        int perProducer = 200;
        int consumersPerTopic = 4;
        int totalPerTopic = producersPerTopic * perProducer;

        for (int t = 0; t < topicCount; t++) {
            createTopic("topic-" + t, totalPerTopic + 500);
        }

        int threadsPerTopic = producersPerTopic + consumersPerTopic;
        ExecutorService pool = Executors.newFixedThreadPool(topicCount * threadsPerTopic);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch allProducers = new CountDownLatch(topicCount * producersPerTopic);

        @SuppressWarnings("unchecked")
        Set<String>[] delivered = new Set[topicCount];
        AtomicInteger[] producedOk = new AtomicInteger[topicCount];
        AtomicBoolean[] producersDone = new AtomicBoolean[topicCount];
        CopyOnWriteArrayList<String> duplicates = new CopyOnWriteArrayList<>();
        for (int t = 0; t < topicCount; t++) {
            delivered[t] = ConcurrentHashMap.newKeySet();
            producedOk[t] = new AtomicInteger();
            producersDone[t] = new AtomicBoolean(false);
        }

        long deadline = System.currentTimeMillis() + 120_000;
        for (int t = 0; t < topicCount; t++) {
            final int topicIdx = t;
            final String topic = "topic-" + t;

            for (int p = 0; p < producersPerTopic; p++) {
                final int producerId = p;
                pool.submit(() -> {
                    await(startGate);
                    try {
                        for (int i = 0; i < perProducer; i++) {
                            ResponseEntity<String> resp = produce(topic, "p" + producerId + "-m" + i);
                            if (resp.getStatusCode() == HttpStatus.OK) {
                                producedOk[topicIdx].incrementAndGet();
                            } else {
                                countIfServerError(resp);
                            }
                        }
                    } finally {
                        allProducers.countDown();
                    }
                });
            }

            for (int c = 0; c < consumersPerTopic; c++) {
                pool.submit(() -> {
                    await(startGate);
                    while (System.currentTimeMillis() < deadline) {
                        if (producersDone[topicIdx].get()
                                && delivered[topicIdx].size() >= producedOk[topicIdx].get()) {
                            break;
                        }
                        ResponseEntity<String> resp = consume(topic);
                        if (resp.getStatusCode() != HttpStatus.OK) {
                            countIfServerError(resp);
                            continue;
                        }
                        String body = resp.getBody();
                        if (body != null && body.startsWith("p")) {
                            if (!delivered[topicIdx].add(body)) {
                                duplicates.add(topic + "/" + body);
                            }
                        }
                    }
                });
            }
        }

        startGate.countDown();
        assertThat(allProducers.await(120, TimeUnit.SECONDS)).isTrue();
        for (AtomicBoolean done : producersDone) {
            done.set(true);
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(120, TimeUnit.SECONDS)).isTrue();

        assertThat(serverErrors.get()).as("no 5xx responses under load").isZero();
        assertThat(duplicates).as("no message delivered more than once across all topics").isEmpty();
        for (int t = 0; t < topicCount; t++) {
            assertThat(producedOk[t].get()).isEqualTo(totalPerTopic);
            assertThat(delivered[t]).as("topic-" + t + " lossless").hasSize(totalPerTopic);
            assertThat(queueMessageRepository.countByTopic("topic-" + t)).isZero();
        }
    }

    private void countIfServerError(ResponseEntity<String> resp) {
        if (resp.getStatusCode().is5xxServerError()) {
            serverErrors.incrementAndGet();
        }
    }

    private static void await(CountDownLatch gate) {
        try {
            gate.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
