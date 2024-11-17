package cis5550.jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URL;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.Row;

public class Crawler {
    private static final String USER_AGENT = "cis5550-crawler";
    private static final Map<String, List<String>> robotsRules = new HashMap<>();
    private static final long RATE_LIMIT_MS = 1000;

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

    private static void fetchRobotsTxt(FlameContext context, String host, String baseUrl) {
        try {
            URL robotsUrl = new URL("http://" + host + "/robots.txt");
            HttpURLConnection conn = (HttpURLConnection) robotsUrl.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", USER_AGENT);

            if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                String robotsTxtContent = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                conn.getInputStream().close();

                robotsRules.put(host, parseRobotsTxt(robotsTxtContent));
                String hostKey = String.valueOf(urlToAbsolute(baseUrl, "http://" + host).hashCode());
                Row hostRow = context.getKVS().getRow("pt-crawl", hostKey);

                if (hostRow == null) {
                    hostRow = new Row(hostKey);
                    hostRow.put("url", urlToAbsolute(baseUrl, "http://" + host));
                }

                hostRow.put("robotsTxt", robotsTxtContent);
                context.getKVS().putRow("pt-crawl", hostRow);
            } else {
                robotsRules.put(host, Collections.emptyList());
            }
        } catch (IOException e) {
            System.out.println("Error fetching robots.txt for host: " + host);
            robotsRules.put(host, Collections.emptyList());
        }
    }

    private static List<String> parseRobotsTxt(String robotsTxtContent) {
        List<String> rules = new ArrayList<>();
        try (Scanner scanner = new Scanner(robotsTxtContent)) {
            boolean applies = false;
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.startsWith("User-agent:")) {
                    String agent = line.substring(11).trim();
                    applies = agent.equals("*") || agent.equalsIgnoreCase(USER_AGENT);
                } else if (applies && (line.startsWith("Disallow:") || line.startsWith("Allow:"))) {
                    rules.add(line);
                }
            }
        }
        return rules;
    }

    private static boolean isUrlAllowed(String host, String urlPath) {
        List<String> rules = robotsRules.get(host);
        for (String rule : rules) {
            String[] parts = rule.split(":", 2);
            if (urlPath.startsWith(parts[1].trim())) {
                return "Allow".equalsIgnoreCase(parts[0].trim());
            }
        }
        return true;
    }

    private static boolean dontProceedDueToRateLimit(FlameContext context, String host)
            throws FileNotFoundException, IOException {
        Row hostRow = context.getKVS().getRow("hosts", host);
        long lastAccessTime = (hostRow != null && hostRow.get("lastAccessTime") != null)
                ? Long.parseLong(hostRow.get("lastAccessTime"))
                : 0;

        long currentTime = Instant.now().toEpochMilli();
        if (currentTime - lastAccessTime < RATE_LIMIT_MS) {
            return true;
        }
        if (hostRow == null) {
            hostRow = new Row(host);
        }

        hostRow.put("lastAccessTime", String.valueOf(currentTime));
        context.getKVS().putRow("hosts", hostRow);
        return false;
    }

    public static void run(FlameContext context, String[] args) throws Exception {
        if (args.length != 1) {
            context.output("Error: A single seed URL is required.");
            return;
        }
        String normalizedSeed = urlToAbsolute(args[0], args[0]);
        System.out.println("Seed url: " + normalizedSeed);

        FlameRDD urlQueue = context.parallelize(Arrays.asList(normalizedSeed));

        while (urlQueue.count() > 0) {
            urlQueue = urlQueue.flatMap(url -> {
                List<String> newUrls = new ArrayList<>();

                // don't recrawl
                String urlHash = String.valueOf(url.hashCode());
                if (context.getKVS().getRow("pt-crawl", urlHash) != null) {
                    return newUrls;
                }

                URL targetUrl = new URL(url);
                String host = targetUrl.getHost();

                if (dontProceedDueToRateLimit(context, host)) {
                    return Arrays.asList(url);
                }

                if (!robotsRules.containsKey(host)) {
                    fetchRobotsTxt(context, host, url);
                }

                // check robots
                if (!isUrlAllowed(host, targetUrl.getPath())) {
                    System.out.println("Disallowed by robots.txt: " + url);
                    return newUrls;
                }

                HttpURLConnection conn = (HttpURLConnection) targetUrl.openConnection();
                conn.setRequestMethod("HEAD");
                conn.setInstanceFollowRedirects(false);
                conn.setRequestProperty("User-Agent", "cis5550-crawler");

                int responseCode = conn.getResponseCode();
                String contentType = conn.getContentType();

                if (responseCode == HttpURLConnection.HTTP_OK && contentType != null
                        && contentType.startsWith("text/html")) {
                    conn = (HttpURLConnection) targetUrl.openConnection();
                    conn.setRequestMethod("GET");
                    conn.setRequestProperty("User-Agent", "cis5550-crawler");

                    responseCode = conn.getResponseCode();
                    contentType = conn.getContentType();
                    int contentLength = conn.getContentLength();

                    if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                        InputStream inputStream = conn.getInputStream();
                        String pageContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                        inputStream.close();
                        Row row = context.getKVS().getRow("pt-crawl", urlHash);
                        if (row == null) {
                            row = new Row(urlHash);
                        }
                        row.put("url", url);
                        row.put("page", pageContent);
                        row.put("responseCode", String.valueOf(responseCode));
                        row.put("contentType", contentType);
                        row.put("length", String.valueOf(contentLength));

                        context.getKVS().putRow("pt-crawl", row);
                        newUrls.addAll(extractUrls(pageContent, url));
                    }
                } else if (responseCode == HttpURLConnection.HTTP_MOVED_PERM ||
                        responseCode == HttpURLConnection.HTTP_MOVED_TEMP ||
                        responseCode == HttpURLConnection.HTTP_SEE_OTHER ||
                        responseCode == 307 || responseCode == 308) {
                    String location = conn.getHeaderField("Location");
                    if (location != null) {
                        Row row = new Row(urlHash);
                        row.put("url", url);
                        row.put("responseCode", String.valueOf(responseCode));
                        context.getKVS().putRow("pt-crawl", row);
                        newUrls.add(urlToAbsolute(url, location));
                    }
                } else {
                    Row row = new Row(urlHash);
                    row.put("url", url);
                    row.put("responseCode", String.valueOf(responseCode));
                    context.getKVS().putRow("pt-crawl", row);
                }

                return newUrls;
            });
            System.out.println("Current queue size: " + urlQueue.count());

            // Thread.sleep(20);
        }
    }
}
