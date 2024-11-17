package cis5550.jobs;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class PageRank {
    public static void run(FlameContext ctx, String[] args) throws Exception {
        // load data / map each row to (u, "1.0,1.0,L")
        FlamePairRDD previousStateTable = ctx.fromTable("pt-crawl", row -> {
            String urlHash = row.key();
            List<String> normalizedLinks = extractUrls(row.get("page"), row.get("url"));
            List<String> linkHashes = new ArrayList<>();
            for (String link : normalizedLinks) {
                linkHashes.add(Hasher.hash(link));
            }
            return urlHash + ",1.0,1.0," + String.join(",", linkHashes);
        }).mapToPair(str -> {
            // split out url hash and data entry
            String[] parts = str.split(",", 2);
            return new FlamePair(parts[0], parts[1]);
        });

        // convergence loop
        double minConvergenceRatio = 1;
        final double CONVERGENCE_THRESH = Double.parseDouble(args[0]);

        if (args.length == 2) {
            minConvergenceRatio = Double.parseDouble(args[1]) / 100.0;
        }
        while (true) {
            FlamePairRDD aggregatedRanks = previousStateTable.flatMapToPair(pair -> {
                List<FlamePair> rankContributions = new ArrayList<>();
                String[] data = pair._2().split(",", 3);
                double currentRank = Double.parseDouble(data[0]);
                String links = data.length > 2 ? data[2] : "";

                // calc rank contribution
                if (!links.isEmpty()) {
                    String[] outgoingLinks = links.split(",");
                    double rankContribution = 0.85 * currentRank / outgoingLinks.length;
                    for (String li : outgoingLinks) {
                        rankContributions.add(new FlamePair(li, Double.toString(rankContribution)));
                    }
                }

                // retain nodes with zero indegree
                rankContributions.add(new FlamePair(pair._1(), "0.0"));
                return rankContributions;
            }).foldByKey("0.0", (acc, value) -> {
                return Double.toString(Double.parseDouble(acc) + Double.parseDouble(value));
            });

            // update state table by joining old state and aggregatedRanks
            FlamePairRDD newStateTable = previousStateTable.join(aggregatedRanks).flatMapToPair(joinedPair -> {
                String dataToSplit = joinedPair._2();
                // the url region can be variable length, so we need to find positions of commas
                int firstComma = dataToSplit.indexOf(",");
                int secondComma = dataToSplit.indexOf(",", firstComma + 1);
                int lastComma = dataToSplit.lastIndexOf(",");

                String oldCurrentRank = dataToSplit.substring(0, firstComma);
                String links = dataToSplit.substring(secondComma + 1, lastComma);
                String newRankStr = dataToSplit.substring(lastComma + 1);

                double newRank = 0.15 + Double.parseDouble(newRankStr);
                // return (hash, newState)
                return Collections
                        .singletonList(new FlamePair(joinedPair._1(), newRank + "," + oldCurrentRank + "," + links));
            });

            previousStateTable = newStateTable;

            // get max difference to check for convergence
            List<String> differences = newStateTable.flatMap(pair -> {
                // diff between this and last rank
                String[] ranks = pair._2().split(",");
                return Collections.singletonList(
                        Double.toString(Math.abs(Double.parseDouble(ranks[0]) - Double.parseDouble(ranks[1]))));
            }).collect();

            int numConverged = 0;
            for (String diff : differences) {
                if (Double.parseDouble(diff) <= CONVERGENCE_THRESH) {
                    numConverged++;
                }
            }

            if ((double) numConverged / differences.size() >= minConvergenceRatio) {
                break;
            }
        }

        // finally save pagerank table
        previousStateTable.flatMapToPair(pair -> {
            Row row = new Row(pair._1());
            row.put("rank", pair._2().split(",", 3)[0]);
            ctx.getKVS().putRow("pt-pageranks", row);
            return Collections.emptyList();
        });
    }

    // old helper functions from hw8:
    private static String urlToAbsolute(String baseUrl, String candidateUrl) {
        try {
            URL absoluteUrl = new URL(new URL(baseUrl), candidateUrl);

            int port = absoluteUrl.getPort();
            if (port == -1) {
                int defaultPort = "http".equalsIgnoreCase(absoluteUrl.getProtocol()) ? 80 : 443;

                absoluteUrl = new URL(absoluteUrl.getProtocol(), absoluteUrl.getHost(), defaultPort,
                        absoluteUrl.getFile());
            }

            // add trailing slash when looking at root
            String path = absoluteUrl.getPath();
            if (path.isEmpty()) {
                path = "/";
                absoluteUrl = new URL(absoluteUrl.getProtocol(), absoluteUrl.getHost(), absoluteUrl.getPort(), path);
            }

            // ignore unwanted protocols & static files
            if (!"http".equalsIgnoreCase(absoluteUrl.getProtocol()) &&
                    !"https".equalsIgnoreCase(absoluteUrl.getProtocol())) {
                return null;
            }
            if (absoluteUrl.getPath().matches(".*\\.(jpg|jpeg|png|gif|svg|css|js)$")) {
                return null;
            }
            return absoluteUrl.toString();
        } catch (MalformedURLException e) {
            System.out.println("malformed url: " + candidateUrl);
            e.printStackTrace();
        }
        return null;
    }

    public static List<String> extractUrls(String pageContent, String baseUrl) {
        List<String> urls = new ArrayList<>();

        // url pattern match ==> match the entire a tag, placing the contents of href in
        // capture group 1 (while ignoring fragment identifiers)
        Pattern pattern = Pattern.compile("<a[^>]*href=[\"']([^\"'#>]+)[\"'][^>]*>", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(pageContent);

        while (matcher.find()) {
            String href = matcher.group(1).split("#")[0].trim();
            String absoluteUrl = urlToAbsolute(baseUrl, href);
            System.out.println("extracting url: " + absoluteUrl);
            urls.add(absoluteUrl);
        }

        return urls;
    }
}
