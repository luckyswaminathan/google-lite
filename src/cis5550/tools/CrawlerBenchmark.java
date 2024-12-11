package cis5550.tools;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.jobs.Crawler;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CrawlerBenchmark {
    private static final Logger logger = Logger.getLogger(CrawlerBenchmark.class);
    private final KVSClient kvsClient;
    private final FlameContext flameContext;
    private final Map<String, List<BenchmarkResult>> results = new ConcurrentHashMap<>();

    public static class BenchmarkResult {
        public final String operation;
        public final double avgLatencyMs;
        public final double p95LatencyMs;
        public final double p99LatencyMs;
        public final int successCount;
        public final int failureCount;
        public final double throughput;  // operations per second

        public BenchmarkResult(String operation, List<Long> latencies, int failures, Duration testDuration) {
            this.operation = operation;
            Collections.sort(latencies);

            this.avgLatencyMs = latencies.stream().mapToLong(l -> l).average().orElse(0.0);
            this.p95LatencyMs = latencies.get((int)(latencies.size() * 0.95));
            this.p99LatencyMs = latencies.get((int)(latencies.size() * 0.99));
            this.successCount = latencies.size();
            this.failureCount = failures;
            this.throughput = (double) successCount / testDuration.toSeconds();
        }
    }

    public CrawlerBenchmark(FlameContext context) {
        this.flameContext = context;
        this.kvsClient = context.getKVS();
    }

    // Test network connectivity between workers
    public void benchmarkWorkerConnectivity(int numWorkers) throws Exception {
        logger.info("Starting worker connectivity benchmark");

        List<String> testData = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            testData.add("test-" + i);
        }

        FlameRDD rdd = flameContext.parallelize(testData);
        Instant start = Instant.now();

        // Force data transfer between workers with a shuffle operation
        FlamePairRDD pairs = rdd.mapToPair(s -> new FlamePair(String.valueOf(s.hashCode() % numWorkers), s));
        long count = pairs.collect().size();

        Duration duration = Duration.between(start, Instant.now());
        addResult("worker_connectivity", List.of(duration.toMillis()), 0, duration);

        logger.info(String.format("Worker connectivity test completed. Processed %d items in %d ms",
                count, duration.toMillis()));
    }

    // Test KVS operations
    public void benchmarkKVS(int numOperations) throws Exception {
        logger.info("Starting KVS benchmark");
        List<Long> putLatencies = new ArrayList<>();
        List<Long> getLatencies = new ArrayList<>();
        int putFailures = 0;
        int getFailures = 0;

        Instant start = Instant.now();

        for (int i = 0; i < numOperations; i++) {
            String key = "bench-" + i;
            Row row = new Row(key);
            row.put("data", "test-value-" + i);

            try {
                Instant putStart = Instant.now();
                kvsClient.putRow("benchmark", row);
                putLatencies.add(Duration.between(putStart, Instant.now()).toMillis());
            } catch (Exception e) {
                putFailures++;
                logger.error("KVS put failed", e);
            }

            try {
                Instant getStart = Instant.now();
                kvsClient.getRow("benchmark", key);
                getLatencies.add(Duration.between(getStart, Instant.now()).toMillis());
            } catch (Exception e) {
                getFailures++;
                logger.error("KVS get failed", e);
            }
        }

        Duration totalDuration = Duration.between(start, Instant.now());
        addResult("kvs_put", putLatencies, putFailures, totalDuration);
        addResult("kvs_get", getLatencies, getFailures, totalDuration);
    }

    // Test HTTP operations
    public void benchmarkHttpOperations(List<String> urls, int concurrency) throws Exception {
        logger.info("Starting HTTP operations benchmark");
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Long> headLatencies = new ArrayList<>();
        List<Long> getLatencies = new ArrayList<>();
        AtomicInteger headFailures = new AtomicInteger();
        AtomicInteger getFailures = new AtomicInteger();

        Instant start = Instant.now();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String url : urls) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    // Benchmark HEAD request
                    Instant headStart = Instant.now();
                    HttpURLConnection headConn = (HttpURLConnection) new URL(url).openConnection();
                    headConn.setRequestMethod("HEAD");
                    headConn.setConnectTimeout(5000);
                    headConn.connect();
                    int responseCode = headConn.getResponseCode();
                    synchronized (headLatencies) {
                        headLatencies.add(Duration.between(headStart, Instant.now()).toMillis());
                    }
                    headConn.disconnect();

                    // Only do GET if HEAD was successful
                    if (responseCode == 200) {
                        Instant getStart = Instant.now();
                        HttpURLConnection getConn = (HttpURLConnection) new URL(url).openConnection();
                        getConn.setRequestMethod("GET");
                        getConn.setConnectTimeout(5000);
                        getConn.getInputStream().close();
                        synchronized (getLatencies) {
                            getLatencies.add(Duration.between(getStart, Instant.now()).toMillis());
                        }
                        getConn.disconnect();
                    }
                } catch (Exception e) {
                    synchronized (CrawlerBenchmark.this) {
                        headFailures.getAndIncrement();
                        getFailures.getAndIncrement();
                    }
                    logger.error("HTTP operation failed for URL: " + url, e);
                }
            }, executor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();

        Duration totalDuration = Duration.between(start, Instant.now());
        addResult("http_head", headLatencies, headFailures.get(), totalDuration);
        addResult("http_get", getLatencies, getFailures.get(), totalDuration);
    }

    // Benchmark URL normalization and extraction
    public void benchmarkUrlProcessing(List<String> urls, String baseUrl) {
        logger.info("Starting URL processing benchmark");
        List<Long> normalizationLatencies = new ArrayList<>();
        int failures = 0;

        Instant start = Instant.now();

        for (String url : urls) {
            try {
                Instant opStart = Instant.now();
                Crawler.normalizeUrl(url, baseUrl);
                normalizationLatencies.add(Duration.between(opStart, Instant.now()).toMillis());
            } catch (Exception e) {
                failures++;
                logger.error("URL normalization failed", e);
            }
        }

        Duration totalDuration = Duration.between(start, Instant.now());
        addResult("url_normalization", normalizationLatencies, failures, totalDuration);
    }

    private void addResult(String operation, List<Long> latencies, int failures, Duration testDuration) {
        results.computeIfAbsent(operation, k -> new ArrayList<>())
                .add(new BenchmarkResult(operation, latencies, failures, testDuration));
    }

    // Get benchmark results
    public Map<String, List<BenchmarkResult>> getResults() {
        return new HashMap<>(results);
    }

    // Print results in a formatted way
    public void printResults() {
        System.out.println("\nCrawler Benchmark Results:");
        System.out.println("==========================");

        results.forEach((operation, resultsList) -> {
            System.out.printf("\nOperation: %s\n", operation);
            System.out.println("-----------------");

            for (int i = 0; i < resultsList.size(); i++) {
                BenchmarkResult result = resultsList.get(i);
                System.out.printf("Run %d:\n", i + 1);
                System.out.printf("  Average Latency: %.2f ms\n", result.avgLatencyMs);
                System.out.printf("  P95 Latency: %.2f ms\n", result.p95LatencyMs);
                System.out.printf("  P99 Latency: %.2f ms\n", result.p99LatencyMs);
                System.out.printf("  Success/Failure: %d/%d\n", result.successCount, result.failureCount);
                System.out.printf("  Throughput: %.2f ops/sec\n", result.throughput);
            }
        });
    }
}