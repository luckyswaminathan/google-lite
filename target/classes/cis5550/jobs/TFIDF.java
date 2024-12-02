package cis5550.jobs;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.tools.Hasher;

public class TFIDF {
    public static void generateTF(FlameContext ctx) throws Exception {
        // document hash -> content
        FlamePairRDD urlPagePairs = ctx.fromTable("pt-crawl", row -> {
            return row.get("url") + "," + row.get("page");
        }).mapToPair(str -> {
            String[] parts = str.split(",", 2);
            return new FlamePair(parts[0], parts[1]);
        });

        // tf for each doc
        FlamePairRDD wordUrlPairs = urlPagePairs.flatMapToPair(pair -> {
            String url = URLDecoder.decode(pair._1(), StandardCharsets.UTF_8);
            String urlHash = Hasher.hash(url);
            String pageContent = pair._2().toLowerCase()
                    .replaceAll("[.,:;!?’\"()\\-]", " ") // remove punctuation
                    .replaceAll("\\s+", " ") // collapse multiple spaces
                    .trim();

            // tokenize
            String[] words = pageContent.split(" ");

            // compute TF for each word
            Map<String, Double> tfWordToCount = new HashMap<>();
            for (String word : words) {
                if (!word.isEmpty()) {
                    tfWordToCount.put(word, tfWordToCount.getOrDefault(word, 0.0) + 1);
                }
            }

            // normalize
            tfWordToCount.replaceAll((_, count) -> count / words.length);
            List<FlamePair> pairs = new ArrayList<>();
            for (Map.Entry<String, Double> entry : tfWordToCount.entrySet()) {
                pairs.add(new FlamePair(urlHash + "-" + entry.getKey(), String.valueOf(entry.getValue())));
            }
            return pairs;
        });

        // aggregate tf for each word across all documents (fold by
        // word)
        FlamePairRDD termFrequencies = wordUrlPairs.foldByKey("", (existingTf, newTf) -> {
            if (existingTf.isEmpty()) {
                return newTf;
            }
            return String.valueOf(Double.parseDouble(existingTf) + Double.parseDouble(newTf));
        });
        termFrequencies.saveAsTable("pt-tf");
    }

    public static void generateIDF(FlameContext ctx) throws Exception {
        FlameRDD documents = ctx.fromTable("pt-crawl", row -> {
            return row.key() + "\t" + row.get("page");
        });

        // flatten into (word, docId) pairs
        FlameRDD wordDocPairs = documents.flatMap(row -> {
            String[] parts = row.split("\t", 2);
            String docId = parts[0];
            String content = parts[1];

            String[] words = content.toLowerCase()
                    .replaceAll("[.,:;!?’\"()\\-]", " ")
                    .replaceAll("\\s+", " ")
                    .trim()
                    .split(" ");

            Set<String> pairs = new HashSet<>();
            for (String word : words) {
                pairs.add(word + "\t" + docId);
            }

            return new ArrayList<>(pairs);
        });

        // (word, docId) pairs to (word, "1") for counting
        FlamePairRDD wordOccurrences = wordDocPairs.mapToPair(pair -> {
            String word = pair.split("\t", 2)[0];
            return new FlamePair(word, "1");
        });

        // now fold to count
        FlamePairRDD wordDocCounts = wordOccurrences.foldByKey("0", (c1, c2) -> {
            return String.valueOf(Integer.parseInt(c1) + Integer.parseInt(c2));
        });

        long totalDocuments = ctx.fromTable("pt-crawl", row -> row.key()).count();
        FlameRDD idfValues = wordDocCounts.flatMap(pair -> {
            int docCount = Integer.parseInt(pair._2());
            return Arrays.asList("" + Math.log((double) totalDocuments / docCount));
        });
        idfValues.saveAsTable("pt-idf");
    }

    public static void run(FlameContext ctx, String[] args) throws Exception {
        generateTF(ctx);
        generateIDF(ctx);
    }
}
