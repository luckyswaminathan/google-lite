package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class Crawler {
    private static final Logger logger = Logger.getLogger(Crawler.class);

    private static final long defaultCrawlDelay = 0; // in milliseconds

    public static void run(FlameContext flameContext, String[] args) throws Exception {
        if (args.length != 1) {
            flameContext.output("Error: Seed URL required");
            return;
        }

        String seedUrl = args[0];
        seedUrl = normalizeUrl(seedUrl, "");
        if (seedUrl == null || seedUrl.isEmpty()) {
            flameContext.output("Error: Invalid seed URL (failed on normalization).");
            return;
        }

        FlameRDD urlQueue = flameContext.parallelize(List.of(seedUrl));

        while (urlQueue.count() > 0) {
            System.out.println("Current queue size: " + urlQueue.count());
            urlQueue = urlQueue.flatMap(url -> {
                List<String> extractedAndNormalizedUrls = new ArrayList<>();
                try {
                    String rowKey = Hasher.hash(url);
                    KVSClient kvsClient = flameContext.getKVS();
                    if (kvsClient.existsRow("pt-crawl", rowKey)) {
                        return extractedAndNormalizedUrls; // Should be empty at this point
                    }

                    // Parse URL to get the host, check that hosts robotsTxt
                    URL urlObj = new URL(url);
                    String host = urlObj.getHost();

                    // Check the robots.txt endpoint for the current URL host
                    Row hostRow = kvsClient.getRow("hosts", host);
                    RobotsTxt robotsInfo;
                    if (hostRow == null || hostRow.get("robotsFetched") == null) {
                        hostRow = hostRow != null ? hostRow : new Row(host);

                        // Fetch, parse, and serialize robots.txt for hosts table storage
                        String robotsTxtContent = fetchRobotsTxt(host);
                        hostRow.put("robotsTxt", robotsTxtContent != null ? robotsTxtContent : "");
                        hostRow.put("robotsFetched", "true"); // Mark that robots.txt has been fetched
                        robotsInfo = parseRobotsTxt(robotsTxtContent);
                        hostRow.put("crawlDelay", String.valueOf(robotsInfo.crawlDelay));
                        hostRow.put("robotsRules", serializeRobotsRules(robotsInfo.rules));
                        kvsClient.putRow("hosts", hostRow);
                    } else {
                        robotsInfo = getRobotsInfoFromHostRow(hostRow);
                    }

                    // Check that last access time was at least currHostCrawlDelay away
                    long currHostCrawlDelay = defaultCrawlDelay;
                    if (hostRow.get("crawlDelay") != null) {
                        currHostCrawlDelay = (long) (Double.parseDouble(hostRow.get("crawlDelay")) * 1000);
                    }

                    long currentTime = System.currentTimeMillis();

                    if (hostRow != null && hostRow.get("lastAccessTime") != null) {
                        long lastAccessTime = Long.parseLong(hostRow.get("lastAccessTime"));
                        if (currentTime - lastAccessTime < currHostCrawlDelay) {
                            // Rate limit reached so return this url for trying next cycle
                            extractedAndNormalizedUrls.add(url);
                            return extractedAndNormalizedUrls;
                        }
                    }

                    // Update last access time for host (done after rate limit check)
                    hostRow.put("lastAccessTime", String.valueOf(currentTime));
                    kvsClient.putRow("hosts", hostRow);

                    // Check if URL allowed by robots.txt, otherwise don't explore this page
                    if (!isUrlAllowed(urlObj, robotsInfo)) {
                        return extractedAndNormalizedUrls; // empty (should be)
                    }

                    HttpURLConnection headConnection = (HttpURLConnection) urlObj.openConnection();
                    headConnection.setRequestMethod("HEAD");
                    headConnection.setRequestProperty("User-Agent", "cis5550-crawler");
                    headConnection.setInstanceFollowRedirects(false); // From Ed post: to not follow re-directs
                    headConnection.connect();

                    Row row = new Row(rowKey);

                    // Add in url, responseCode, contentType, length metadata
                    row.put("url", url);

                    int responseCode = headConnection.getResponseCode();
                    row.put("responseCode", String.valueOf(responseCode));

                    String contentType = headConnection.getContentType();
                    if (contentType != null) {
                        row.put("contentType", contentType);
                    }

                    if (isRedirect(responseCode)) {
                        String locationHeader = headConnection.getHeaderField("Location");
                        if (locationHeader != null && !locationHeader.isEmpty()) {
                            String nextUrl = normalizeUrl(locationHeader, url);
                            if (nextUrl != null && !nextUrl.isEmpty()) {
                                extractedAndNormalizedUrls.add(nextUrl);
                            }
                        }
                    } else {
                        // Do GET request only if responseCode on HEAD is 200 AND contentType is
                        // text/html
                        if (responseCode == HttpURLConnection.HTTP_OK && contentType != null
                                && contentType.equalsIgnoreCase("text/html")) {
                            HttpURLConnection getConnection = (HttpURLConnection) urlObj.openConnection();
                            getConnection.setRequestMethod("GET");
                            getConnection.setRequestProperty("User-Agent", "cis5550-crawler");
                            getConnection.connect();

                            int length = getConnection.getContentLength();
                            byte[] pageContentAsBytes = getPageContentAsBytes(getConnection);

                            if (length != -1) {
                                row.put("length", String.valueOf(length));
                            }
                            row.put("page", pageContentAsBytes);

                            String pageContent = new String(pageContentAsBytes, StandardCharsets.UTF_8);
                            extractedAndNormalizedUrls.addAll(extractNormalizedUrls(pageContent, url));
                            getConnection.disconnect();
                        }
                    }
                    kvsClient.putRow("pt-crawl", row);
                    headConnection.disconnect();
                } catch (Exception e) {
                    logger.error("Error while running crawler run function; Current URL: " + url, e);
                }
                return extractedAndNormalizedUrls;
            });

            // Sleep to prevent too-quick loops during testing
            // try {
            // Thread.sleep(500);
            // } catch (InterruptedException e) {
            // Thread.currentThread().interrupt();
            // }
        }
    }

    private static byte[] getPageContentAsBytes(HttpURLConnection connection) throws IOException {
        InputStream inStream = connection.getInputStream();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int bytesRead;
        byte[] data = new byte[1024];

        while ((bytesRead = inStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, bytesRead);
        }

        buffer.flush();
        inStream.close();
        return buffer.toByteArray();
    }

    public static List<String> extractNormalizedUrls(String pageContent, String baseUrl) {
        List<String> extractedAndNormalizedUrls = new ArrayList<>();

        List<String> rawExtractedUrls = extractRawUrls(pageContent);
        for (String rawExtractedUrl : rawExtractedUrls) {
            try {
                String normalizedUrl = normalizeUrl(rawExtractedUrl, baseUrl);
                if (normalizedUrl != null && !normalizedUrl.isEmpty()) {
                    extractedAndNormalizedUrls.add(normalizedUrl);
                }
            } catch (Exception e) {
                logger.error("Error extracting and normalizing URLs", e);
            }
        }
        return extractedAndNormalizedUrls;
    }

    private static List<String> extractRawUrls(String pageContent) {
        List<String> extractedRawUrls = new ArrayList<>();

        int nextSearchStart = 0;
        while (nextSearchStart < pageContent.length()) {
            int startOfTag = pageContent.indexOf('<', nextSearchStart);
            if (startOfTag == -1) {
                break;
            }

            int endOfTag = pageContent.indexOf('>', startOfTag + 1);
            if (endOfTag == -1) {
                break;
            }

            String contentInTag = pageContent.substring(startOfTag + 1, endOfTag);
            contentInTag = contentInTag.trim();

            if (contentInTag.startsWith("/")) {
                nextSearchStart = endOfTag + 1;
                continue;
            }

            String[] splitTag = contentInTag.split("\\s+");
            if (splitTag.length == 0) {
                nextSearchStart = endOfTag + 1;
                continue;
            }

            String tagType = splitTag[0].toLowerCase();
            if (!tagType.equalsIgnoreCase("a")) {
                nextSearchStart = endOfTag + 1;
                continue;
            }

            String hrefVal = null;
            String attributesAsString = contentInTag.substring(tagType.length()).trim();

            int posInAttributes = 0;
            while (posInAttributes < attributesAsString.length()) {
                if (Character.isWhitespace(attributesAsString.charAt(posInAttributes))) {
                    posInAttributes++;
                    continue;
                }

                int posOfNextEqual = attributesAsString.indexOf('=', posInAttributes);
                if (posOfNextEqual == -1) {
                    break;
                }

                // Getting Attribute Name
                String attrName = attributesAsString.substring(posInAttributes, posOfNextEqual).trim().toLowerCase();
                posInAttributes = posOfNextEqual + 1;

                // Getting Attribute Value
                String attrValue = null;
                if (posInAttributes < attributesAsString.length()) {
                    char firstChar = attributesAsString.charAt(posInAttributes);
                    if (firstChar == '"' || firstChar == '\'') { // Quote around attribute val
                        posInAttributes++;
                        int endQuotePos = attributesAsString.indexOf(firstChar, posInAttributes); // Look for end quote
                        if (endQuotePos == -1) {
                            break;
                        }
                        attrValue = attributesAsString.substring(posInAttributes, endQuotePos);
                        posInAttributes = endQuotePos + 1;
                    } else { // No quote around attribute val
                        int valueEnd = posInAttributes;
                        while (valueEnd < attributesAsString.length()
                                && !Character.isWhitespace(attributesAsString.charAt(valueEnd))) {
                            valueEnd++;
                        }
                        attrValue = attributesAsString.substring(posInAttributes, valueEnd);
                        posInAttributes = valueEnd; // Pos of first whitespace character after unquoted attribute value
                    }
                }

                if (attrName.equals("href") && attrValue != null) {
                    hrefVal = attrValue;
                    break;
                }
            }

            if (hrefVal != null) {
                extractedRawUrls.add(hrefVal);
            }

            nextSearchStart = endOfTag + 1;
        }

        return extractedRawUrls;
    }

    public static String normalizeUrl(String rawExtractedUrl, String baseUrl) {
        try {
            // Remove in-page URL fragment of baseURL (maybe won't be used at all)
            int fragmentPos1 = baseUrl.indexOf('#');
            if (fragmentPos1 != -1) {
                baseUrl = baseUrl.substring(0, fragmentPos1);
            }

            // Use URLParser to parse baseUrl
            String[] baseUrlParts = URLParser.parseURL(baseUrl);
            String baseUrlHttpProtocol = baseUrlParts[0];
            String baseUrlHost = baseUrlParts[1];
            String baseUrlPort = baseUrlParts[2];
            String baseUrlPath = baseUrlParts[3];

            int fragmentPos2 = rawExtractedUrl.indexOf('#');
            if (fragmentPos2 != -1) {
                rawExtractedUrl = rawExtractedUrl.substring(0, fragmentPos2);
            }
            if (rawExtractedUrl.isEmpty()) {
                return null; // Invalid since staying same page; No need to re-check so return null
            }

            // Use URLParser to parse rawExtractedUrl
            String[] rawExtractedUrlParts = URLParser.parseURL(rawExtractedUrl);
            String rawExtractedUrlHttpProtocol = rawExtractedUrlParts[0];
            String rawExtractedUrlHost = rawExtractedUrlParts[1];
            String rawExtractedUrlPort = rawExtractedUrlParts[2];
            String rawExtractedUrlPath = rawExtractedUrlParts[3];

            // **** Normalized URL construction below ****
            String normalizedUrlHttpProtocol;
            String normalizedUrlHost;
            String normalizedUrlPort;
            String normalizedUrlPath;

            // Handle setting normalizedUrlHttpProtocol
            if (rawExtractedUrlHttpProtocol != null && !rawExtractedUrlHttpProtocol.isEmpty()) {
                normalizedUrlHttpProtocol = rawExtractedUrlHttpProtocol.toLowerCase();
            } else if (baseUrlHttpProtocol != null && !baseUrlHttpProtocol.isEmpty()) {
                normalizedUrlHttpProtocol = baseUrlHttpProtocol.toLowerCase();
            } else {
                return null; // Invalid since neither URLs specify a protocol
            }

            // Handle setting normalizedUrlHost
            if (rawExtractedUrlHost != null && !rawExtractedUrlHost.isEmpty()) {
                normalizedUrlHost = rawExtractedUrlHost.toLowerCase();
            } else if (baseUrlHost != null && !baseUrlHost.isEmpty()) {
                normalizedUrlHost = baseUrlHost.toLowerCase();
            } else {
                return null; // Invalid since neither URLs specify a host
            }

            // Handle setting normalizedUrlPort
            if ((rawExtractedUrlHost != null && rawExtractedUrlHost.equalsIgnoreCase(normalizedUrlHost))
                    && (rawExtractedUrlPort != null && !rawExtractedUrlPort.isEmpty())) {
                normalizedUrlPort = rawExtractedUrlPort;
            } else if ((baseUrlHost != null && baseUrlHost.equalsIgnoreCase(normalizedUrlHost))
                    && (baseUrlPort != null && !baseUrlPort.isEmpty())) {
                normalizedUrlPort = baseUrlPort;
            } else {
                // Default ports based on protocol if not specified by input URLs
                if ("http".equalsIgnoreCase(normalizedUrlHttpProtocol)) {
                    normalizedUrlPort = "80";
                } else if ("https".equalsIgnoreCase(normalizedUrlHttpProtocol)) {
                    normalizedUrlPort = "443";
                } else {
                    return null; // Invalid since URLs not HTTP or HTTPS should be skipped
                }
            }

            // Resolve the path and handle setting normalizedUrlPath
            if (rawExtractedUrlPath != null && !rawExtractedUrlPath.isEmpty()) {
                if (rawExtractedUrlPath.startsWith("/")) {
                    // This is an absolute path, simply set normalizedUrlPath to it
                    normalizedUrlPath = rawExtractedUrlPath;
                } else {
                    // Coming here means this is a relative path

                    // First get the base path (last slash)
                    String basePath = baseUrlPath;
                    int lastSlashIndex = basePath.lastIndexOf('/');
                    if (lastSlashIndex != -1) {
                        basePath = basePath.substring(0, lastSlashIndex + 1);
                    } else {
                        basePath = "/";
                    }

                    // Then, get the combined path and normalize it
                    String combinedPath = basePath + rawExtractedUrlPath;
                    normalizedUrlPath = normalizePath(combinedPath);
                }
            } else if ((baseUrlHost != null && baseUrlHost.equalsIgnoreCase(normalizedUrlHost))
                    && (baseUrlPath != null && !baseUrlPath.isEmpty())) {
                // No need to normalize baseUrlPath since baseUrl is already normalized
                normalizedUrlPath = baseUrlPath;
            } else {
                // Default just to having no additional path on top of the normalized host:port
                normalizedUrlPath = "";
            }

            // Reconstruct the normalized URL
            String normalizedUrl = normalizedUrlHttpProtocol + "://" + normalizedUrlHost + ":" + normalizedUrlPort
                    + normalizedUrlPath;

            // Filter URLs based on protocol and URL doc type
            if (!"http".equalsIgnoreCase(normalizedUrlHttpProtocol)
                    && !"https".equalsIgnoreCase(normalizedUrlHttpProtocol)) {
                return null;
            }
            if (normalizedUrlPath.matches(".*\\.(jpg|jpeg|gif|png|txt)$")) {
                return null;
            }

            return normalizedUrl;
        } catch (Exception e) {
            // Handle exceptions
            e.printStackTrace();
            return null;
        }
    }

    private static String normalizePath(String path) {
        String[] segments = path.split("/");
        Stack<String> pathStack = new Stack<>();
        for (String segment : segments) {
            if (segment.equals("..")) {
                if (!pathStack.isEmpty()) {
                    pathStack.pop();
                }
            } else if (!segment.equals(".") && !segment.isEmpty()) {
                pathStack.push(segment);
            }
        }
        StringBuilder normalizedPath = new StringBuilder();
        for (String segment : pathStack) {
            normalizedPath.append("/").append(segment);
        }

        return normalizedPath.toString();
    }

    private static boolean isRedirect(int responseCode) {
        return responseCode == HttpURLConnection.HTTP_MOVED_PERM || // 301
                responseCode == HttpURLConnection.HTTP_MOVED_TEMP || // 302
                responseCode == HttpURLConnection.HTTP_SEE_OTHER || // 303
                responseCode == 307 || // 307
                responseCode == 308; // 308
    }

    // Fetch robots.txt from the host
    private static String fetchRobotsTxt(String host) {
        try {
            URL robotsUrl = new URL("http://" + host + "/robots.txt");
            HttpURLConnection connection = (HttpURLConnection) robotsUrl.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("User-Agent", "cis5550-crawler");
            connection.connect();

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                InputStream inStream = connection.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inStream, StandardCharsets.UTF_8));
                StringBuilder robotsTxtContent = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    robotsTxtContent.append(line).append("\n");
                }
                reader.close();
                connection.disconnect();
                return robotsTxtContent.toString();
            } else {
                connection.disconnect();
                return null;
            }
        } catch (Exception e) {
            logger.error("Error fetching robots.txt from host: " + host, e);
            return null;
        }
    }

    // Parse robots.txt content and extract each rule
    private static RobotsTxt parseRobotsTxt(String content) {
        RobotsTxt robotsTxtObj = new RobotsTxt();
        if (content == null || content.isEmpty()) {
            return robotsTxtObj;
        }

        String[] lines = content.split("\n");
        String currentUserAgent;
        boolean relevantUserAgentFound = false;
        boolean collectRules = false;

        for (String line : lines) {
            line = line.trim();

            // Ignore comments, empty lines, and lines without a colon
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            int colonIndex = line.indexOf(":");
            if (colonIndex == -1) {
                continue;
            }

            String currentLineRuleType = line.substring(0, colonIndex).trim().toLowerCase();
            String value = line.substring(colonIndex + 1).trim();

            if (currentLineRuleType.equals("user-agent")) {
                // Set up collecting so that we collect rules if under *,
                // unless (or until) we have already found (or later find) cis5550-crawler
                // user-agent rules
                // If already collected for * user-agent & then we find cis5550-crawler, we
                // clear the already collected rules

                currentUserAgent = value.toLowerCase();
                if (currentUserAgent.equals("cis5550-crawler")) {
                    // Clear all past collected rules (from * user-agent) since we found specific
                    // cis5550-crawler rules
                    relevantUserAgentFound = true;
                    collectRules = true;
                    robotsTxtObj.rules.clear();
                } else {
                    // Only collect rules under * if not found cis5550-crawler rules (yet)
                    collectRules = currentUserAgent.equals("*") && !relevantUserAgentFound;
                }
            } else if (collectRules) {
                switch (currentLineRuleType) {
                    case "disallow" -> robotsTxtObj.rules.add(new RobotsTxtRule("disallow", value));
                    case "allow" -> robotsTxtObj.rules.add(new RobotsTxtRule("allow", value));
                    case "crawl-delay" -> {
                        try {
                            robotsTxtObj.crawlDelay = Double.parseDouble(value);
                        } catch (NumberFormatException e) {
                            logger.info("Crawl delay parsing failed, defaulting to: " + defaultCrawlDelay
                                    + " milliseconds");
                        }
                    }
                }
            }
        }

        return robotsTxtObj;
    }

    // Serialize robotsTxt rules into string for storage
    private static String serializeRobotsRules(List<RobotsTxtRule> rules) {
        StringBuilder sb = new StringBuilder();
        for (RobotsTxtRule rule : rules) {
            sb.append(rule.type).append("::").append(rule.ruleInfo).append(",,");
        }
        return sb.toString();
    }

    // Deserialize robotTxt rules from stored string
    private static List<RobotsTxtRule> deserializeRobotsRules(String data) {
        List<RobotsTxtRule> rules = new ArrayList<>();
        String[] lines = data.split(",,");
        for (String line : lines) {
            int separatorIndex = line.indexOf("::");
            if (separatorIndex != -1) {
                String type = line.substring(0, separatorIndex);
                String ruleInfo = line.substring(separatorIndex + 2);
                rules.add(new RobotsTxtRule(type, ruleInfo));
            }
        }
        return rules;
    }

    // Get RobotsTxtInfo object info from hostRow
    private static RobotsTxt getRobotsInfoFromHostRow(Row hostRow) {
        RobotsTxt robotsInfo = new RobotsTxt();

        if (hostRow.get("crawlDelay") != null) {
            try {
                robotsInfo.crawlDelay = Double.parseDouble(hostRow.get("crawlDelay"));
            } catch (NumberFormatException e) {
                logger.info("Crawl delay parsing failed when reading from host table, defaulting to: "
                        + defaultCrawlDelay + " milliseconds");
            }
        }

        if (hostRow.get("robotsRules") != null) {
            robotsInfo.rules = deserializeRobotsRules(hostRow.get("robotsRules"));
        }

        return robotsInfo;
    }

    // Check if the URL is allowed according to robotsTxt
    private static boolean isUrlAllowed(URL url, RobotsTxt robotsTxtObj) {
        String path = url.getPath();

        // Use the first matching rule, or allow by default if no rule found
        for (RobotsTxtRule rule : robotsTxtObj.rules) {
            if (path.startsWith(rule.ruleInfo)) {
                // prefix matched based on the rule, so simply whether this is an allow or
                // disallow rule
                return rule.type.equals("allow");
            }
        }

        return true;
    }

    // Classes for storing robotsTxt as an object, and storing rules as components
    private static class RobotsTxt {
        public List<RobotsTxtRule> rules = new ArrayList<>();
        public double crawlDelay = -1; // -1 no crawl-delay specified
    }

    private static class RobotsTxtRule {
        public String type;
        public String ruleInfo;

        public RobotsTxtRule(String type, String ruleInfo) {
            this.type = type;
            this.ruleInfo = ruleInfo;
        }
    }
}
