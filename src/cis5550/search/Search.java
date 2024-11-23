package cis5550.search;

import cis5550.kvs.KVS;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Search {
    // simple search
    // assumes existence of pt-index, pt-pageranks, pt-idf, pt-tf tables
    // generate these with indexer, pagerank, and TFIDF if missing
    // input is string of words (sent from frontend), and integer n (number of
    // search results)
    // output is list of links (sent to frontend) of length <= n
    //
    public static List<String> simpleSearchWithTFIDF(KVS kvs, String searchString, int numResults) throws Exception {
        // normalize and split the search string into words
        String[] searchWords = searchString.toLowerCase()
                .replaceAll("[.,:;!?’\"()\\-]", " ")
                .replaceAll("\\s+", " ")
                .trim()
                .split(" ");

        Set<String> wordSet = new HashSet<>(Arrays.asList(searchWords));
        Map<String, Double> finalScores = new HashMap<>();
        double alpha = 0.95; // weight for combining TF/IDF and pagerank

        // Scan the "pt-index" table to find matching words and their associated URLs

        for (String word : wordSet) {
            // grab all urls in the index for this word
            byte[] urlsBytes = kvs.get("pt-index", word, "result");
            if (urlsBytes == null) {
                continue;
            }
            String[] urls = new String(urlsBytes, StandardCharsets.UTF_8).split(",");

            // Retrieve the IDF for the word
            double idf = 0.0;
            if (kvs.existsRow("pt-idf", word)) {
                Row r = kvs.getRow("pt-idf", word);
                byte[] idfBytes = kvs.get("pt-idf", word, r.columns().iterator().next());
                if (idfBytes != null) {
                    idf = Double.parseDouble(new String(idfBytes, StandardCharsets.UTF_8));
                }
            }

            // process each URL
            for (String encodedUrl : urls) {
                String url = URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8);
                String urlHash = Hasher.hash(url);

                // pagerank from urlhash
                double pageRank = 0.0;
                if (kvs.existsRow("pt-pageranks", urlHash)) {
                    byte[] rankBytes = kvs.get("pt-pageranks", urlHash, "rank");
                    if (rankBytes != null) {
                        pageRank = Double.parseDouble(new String(rankBytes, StandardCharsets.UTF_8));
                    }
                }

                // tf from word and urlhash
                double tf = 0.0;
                String tfRowKey = urlHash + "-" + word;
                if (kvs.existsRow("pt-tf", tfRowKey)) {
                    byte[] tfBytes = kvs.get("pt-tf", tfRowKey,
                            kvs.getRow("pt-tf", tfRowKey).columns().iterator().next());
                    if (tfBytes != null) {
                        tf = Double.parseDouble(new String(tfBytes, StandardCharsets.UTF_8));
                    }
                }

                // compute score
                double score = alpha * tf * idf + (1 - alpha) * pageRank;
                // acculate scores for URLs across words
                finalScores.merge(url, score, Double::sum);
            }
        }

        List<String> results = finalScores.entrySet().stream()
                .sorted((e1, e2) -> Double.compare(e2.getValue(), e1.getValue())) // sort by descending score
                .limit(numResults)
                .map(entry -> entry.getKey() + " FinalScore: " + entry.getValue())
                .toList();
        return results;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Syntax: search <kvsCoordinator>");
            System.exit(1);
        }

        KVSClient kvs = new KVSClient(args[0]);

        String searchString = "moscow is";
        long startTime = System.currentTimeMillis();
        List<String> searchResults = simpleSearchWithTFIDF(kvs, searchString, 10);
        long endTime = System.currentTimeMillis();
        System.out.println("TFIDF/PageRank Search Results:");
        if (searchResults != null) {
            for (String s : searchResults) {
                System.out.println(s);
            }
        }
        System.out.println("Time taken to search: " + (endTime - startTime) + " millseconds");

    }
    // TODO, replace pagerank helper functions

}
