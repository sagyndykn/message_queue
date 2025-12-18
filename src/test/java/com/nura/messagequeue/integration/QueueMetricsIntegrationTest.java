package com.nura.messagequeue.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class QueueMetricsIntegrationTest extends AbstractMongoIntegrationTest {

    private static final int MESSAGES = 2_000;
    private static final int CONCURRENT_CONSUMERS = 10;

    @Test
    @DisplayName("measure produce and consume throughput/latency")
    void measureThroughputAndLatency() throws Exception {
        createTopic("metrics", MESSAGES + 1000);

        List<Long> produceLatencies = new ArrayList<>(MESSAGES);
        long produceStart = System.nanoTime();
        for (int i = 0; i < MESSAGES; i++) {
            long t0 = System.nanoTime();
            assertThat(produce("metrics", "m" + i).getStatusCode()).isEqualTo(HttpStatus.OK);
            produceLatencies.add(System.nanoTime() - t0);
        }
        long produceElapsedMs = (System.nanoTime() - produceStart) / 1_000_000;
        assertThat(queueMessageRepository.countByTopic("metrics")).isEqualTo(MESSAGES);

        int attemptsPerConsumer = (MESSAGES / CONCURRENT_CONSUMERS) + 20;
        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENT_CONSUMERS);
        List<Long> consumeLatencies = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger consumed = new AtomicInteger();

        long consumeStart = System.nanoTime();
        for (int c = 0; c < CONCURRENT_CONSUMERS; c++) {
            pool.submit(() -> {
                for (int i = 0; i < attemptsPerConsumer; i++) {
                    long t0 = System.nanoTime();
                    String body = consume("metrics").getBody();
                    long elapsed = System.nanoTime() - t0;
                    if (body != null && body.startsWith("m")) {
                        consumeLatencies.add(elapsed);
                        consumed.incrementAndGet();
                    }
                }
            });
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(120, TimeUnit.SECONDS)).isTrue();
        long consumeElapsedMs = (System.nanoTime() - consumeStart) / 1_000_000;

        assertThat(consumed.get()).isEqualTo(MESSAGES);
        assertThat(queueMessageRepository.countByTopic("metrics")).isZero();

        String report = buildReport(produceElapsedMs, produceLatencies, consumeElapsedMs, consumeLatencies);
        System.out.println(report);
        writeReport(report);
    }

    private String buildReport(long produceElapsedMs, List<Long> produceLatencies,
                               long consumeElapsedMs, List<Long> consumeLatencies) {
        double produceThroughput = MESSAGES / (produceElapsedMs / 1000.0);
        double consumeThroughput = MESSAGES / (consumeElapsedMs / 1000.0);

        StringBuilder sb = new StringBuilder();
        sb.append("# Queue Metrics (MongoDB backend)\n\n");
        sb.append("_Generated: ")
                .append(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
                .append("_\n\n");
        sb.append("- Messages: ").append(MESSAGES).append("\n");
        sb.append("- MongoDB: external `mongo:7.0` on localhost (single node)\n");
        sb.append("- Transport: real HTTP via TestRestTemplate (full Spring Boot app)\n\n");

        sb.append("| Operation | Mode | Throughput (msg/s) | Total (ms) | avg (ms) | p50 (ms) | p95 (ms) | p99 (ms) |\n");
        sb.append("|---|---|---|---|---|---|---|---|\n");
        sb.append(row("Produce", "sequential", produceThroughput, produceElapsedMs, produceLatencies));
        sb.append(row("Consume", CONCURRENT_CONSUMERS + " threads", consumeThroughput, consumeElapsedMs, consumeLatencies));
        sb.append("\n");
        sb.append("**Correctness:** ").append(MESSAGES)
                .append(" produced, ").append(MESSAGES)
                .append(" consumed, 0 duplicates, queue fully drained.\n");
        return sb.toString();
    }

    private String row(String op, String mode, double throughput, long totalMs, List<Long> latenciesNanos) {
        List<Long> sorted = new ArrayList<>(latenciesNanos);
        Collections.sort(sorted);
        return String.format(java.util.Locale.US, "| %s | %s | %.0f | %d | %.2f | %.2f | %.2f | %.2f |%n",
                op, mode, throughput, totalMs,
                avgMs(sorted), percentileMs(sorted, 50), percentileMs(sorted, 95), percentileMs(sorted, 99));
    }

    private double avgMs(List<Long> nanos) {
        return nanos.stream().mapToLong(Long::longValue).average().orElse(0) / 1_000_000.0;
    }

    private double percentileMs(List<Long> sortedNanos, int percentile) {
        if (sortedNanos.isEmpty()) {
            return 0;
        }
        int idx = (int) Math.ceil((percentile / 100.0) * sortedNanos.size()) - 1;
        idx = Math.max(0, Math.min(idx, sortedNanos.size() - 1));
        return sortedNanos.get(idx) / 1_000_000.0;
    }

    private void writeReport(String report) throws IOException {
        Path out = Path.of("target", "queue-metrics.md");
        Files.createDirectories(out.getParent());
        Files.writeString(out, report);
        System.out.println("Metrics written to: " + out.toAbsolutePath());
    }
}
