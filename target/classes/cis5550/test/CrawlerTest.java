package cis5550.test;

import cis5550.flame.FlameContext;
import cis5550.tools.CrawlerBenchmark;
import java.util.*;

public class CrawlerTest {
    public static void runBenchmarks(FlameContext context) throws Exception {
        CrawlerBenchmark benchmark = new CrawlerBenchmark(context);

        // Test 1: Worker Connectivity
        System.out.println("Testing worker connectivity...");
        benchmark.benchmarkWorkerConnectivity(3); // Adjust number of workers as needed

        // Test 2: KVS Operations
        System.out.println("\nTesting KVS operations...");
        benchmark.benchmarkKVS(1000);

        // Test 3: HTTP Operations
        System.out.println("\nTesting HTTP operations...");
        List<String> testUrls = Arrays.asList(
                "http://example.com",
                "https://www.upenn.edu",
                "https://www.cis.upenn.edu"
                // Add more test URLs as needed
        );
        benchmark.benchmarkHttpOperations(testUrls, 4);

        // Test 4: URL Processing
        System.out.println("\nTesting URL processing...");
        List<String> urlsToNormalize = Arrays.asList(
                "/relative/path",
                "../parent/path",
                "https://example.com/path?param=value",
                "//example.com/path"
                // Add more test URLs as needed
        );
        benchmark.benchmarkUrlProcessing(urlsToNormalize, "https://base.example.com/page");

        // Print results
        benchmark.printResults();
    }
}