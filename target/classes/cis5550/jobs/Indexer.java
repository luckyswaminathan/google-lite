package cis5550.jobs;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;

public class Indexer {

    public static void run(FlameContext ctx, String[] args) throws Exception {
        FlamePairRDD urlPagePairs = ctx.fromTable("pt-crawl", row -> {
            return row.get("url") + "," + row.get("page");
        }).mapToPair(str -> {
            String[] parts = str.split(",", 2);
            return new FlamePair(parts[0], parts[1]);
        });

        // inverted index
        FlamePairRDD wordUrlPairs = urlPagePairs.flatMapToPair(pair -> {
            String url = pair._1();
            String pageContent = pair._2().toLowerCase()
                    .replaceAll("<[^>]+>", "") // strip tags
                    .replaceAll("[.,:;!?’\"()\\-]", " ") // strip punctuation
                    .replaceAll("\\s+", " "); // collapse whitespace

            // split into duplicate free list of words
            Set<String> uniqueWords = new HashSet<>(Arrays.asList(pageContent.split(" ")));

            return uniqueWords.stream()
                    .filter(word -> !word.isEmpty())
                    .map(word -> new FlamePair(word, url))
                    .toList();
        });
        // fold into the index
        FlamePairRDD invertedIndex = wordUrlPairs.foldByKey("", (urls, url) -> {
            if (urls.isEmpty())
                return url;
            return urls.contains(url) ? urls : urls + "," + url;
        });
        invertedIndex.saveAsTable("pt-index");
    }
}