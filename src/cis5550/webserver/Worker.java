package cis5550.webserver;

import cis5550.tools.Logger;

import javax.net.ssl.SSLSocket;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Worker implements Runnable {
    private static final Logger logger = Logger.getLogger(Worker.class);
    private static final String[] ALLOWED_METHODS = {"GET", "HEAD", "POST", "PUT"};
    private static final String[] FORBIDDEN_METHODS = {};
    private static final DateTimeFormatter rfc1123Formatter =
            DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneId.of("GMT")); // Standard HTTP date format

    private final Socket clientSocket;
    private final String homeDirectory;

    public Worker(Socket clientSocket, String homeDirectory) {
        this.clientSocket = clientSocket;
        this.homeDirectory = homeDirectory;
    }

    @Override
    public void run() {
        try (
                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream();
        ) {
            handleRequest(inputStream, outputStream);
        } catch (IOException ex) {
            logger.error("Error handling client request: " + ex.getMessage(), ex);
        } finally {
            closeSocket();
        }
    }

    private void handleRequest(InputStream inputStream, OutputStream outputStream) throws IOException {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

        String requestLine;
        while (true) {
            requestLine = readLine(bufferedInputStream);
            if (requestLine == null) {
                // End of stream, close connection
                return;
            }
            if (!requestLine.isEmpty()) {
                // Found non-empty line, continue
                break;
            }
            // If the line is empty, continue to read the next line
        }

        logger.info("Request line: " + requestLine);

        // Bad Request Line - not 3 parts
        String[] requestParts = requestLine.split(" ");
        if (requestParts.length != 3) {
            sendErrorResponse(outputStream, 400, "Bad Request");
            return;
        }

        String method = requestParts[0];
        String uri = requestParts[1];
        String protocol = requestParts[2];

        // Check for forbidden methods
        if (isMethodForbidden(method)) {
            sendErrorResponse(outputStream, 405, "Method Not Allowed");
            return;
        }

        // Check if the method is implemented
        if (!isMethodAllowed(method)) {
            sendErrorResponse(outputStream, 501, "Not Implemented");
            return;
        }

        // Verify protocol version
        if (!protocol.equalsIgnoreCase("HTTP/1.1")) {
            sendErrorResponse(outputStream, 505, "HTTP Version Not Supported");
            return;
        }



        // Store headers in below map
        Map<String, String> headersMap = new HashMap<>();
        int contentLength = 0;
        String modifiedSinceHeader = null;

        // Read and process headers
        String line;
        while ((line = readLine(bufferedInputStream)) != null && !line.isEmpty()) {
            logger.info("Header: " + line);
            String[] headerParts = line.split(":", 2);
            if (headerParts.length == 2) {
                String headerName = headerParts[0].trim().toLowerCase();
                String headerValue = headerParts[1].trim();
                headersMap.put(headerName, headerValue);
                switch (headerName) {
                    case "content-length" -> contentLength = Integer.parseInt(headerValue);
                    case "if-modified-since" -> modifiedSinceHeader = headerValue;
                }
            }
        }

        if (!headersMap.containsKey("host")) {
            sendErrorResponse(outputStream, 400, "Bad Request");
            return;
        }

        // Read body using contentLength
        byte[] bodyBytes = null;
        if (contentLength > 0) {
            bodyBytes = new byte[contentLength];
            int totalBytesRead = 0;
            while (totalBytesRead < contentLength) {
                int bytesRead = bufferedInputStream.read(bodyBytes, totalBytesRead, contentLength - totalBytesRead);
                if (bytesRead == -1) {
                    // End of stream reached but shouldn't have
                    sendErrorResponse(outputStream, 400, "Bad Request");
                    return;
                }
                totalBytesRead += bytesRead;
            }
            logger.info("Body read with length: " + totalBytesRead);
        } else {
            bodyBytes = new byte[0];
        }

        String contentTypeHeader = headersMap.get("content-type");
        Map<String, String> bodyParams = new HashMap<>();
        if ("application/x-www-form-urlencoded".equalsIgnoreCase(contentTypeHeader) && bodyBytes.length > 0) {
            String bodyString = new String(bodyBytes, StandardCharsets.UTF_8);
            bodyParams = parseQueryString(bodyString);
        }

        Map<String, String> queryParams = parseQueryParams(uri);
        queryParams.putAll(bodyParams);

        // Check for matching routes
        boolean routeMatched = false;
        List<RouteEntry> routes = Server.getRoutes(); // Get the routes from the Server class
        for (RouteEntry routeEntry : routes) {
            if (routeEntry.method().equalsIgnoreCase(method) && routeEntry.matches(uri)) {
                routeMatched = true;

                // Extract path parameters
                Map<String, String> params = routeEntry.extractParams(uri);

                // Instantiate Request and Response objects
                InetSocketAddress remoteAddress = (InetSocketAddress) clientSocket.getRemoteSocketAddress();

                ResponseImpl response = new ResponseImpl(outputStream);

                RequestImpl request = new RequestImpl(
                        method,
                        uri,
                        protocol,
                        headersMap,
                        queryParams,
                        params,
                        remoteAddress,
                        bodyBytes,
                        Server.getServer(),
                        response, // passing response into request for session cookie handling
                        clientSocket instanceof SSLSocket
                );


                try {
                    Object result = routeEntry.handler().handle(request, response);
                    if (!response.isWriteCalled()) {
                        // Determine response body according to the rules, including the case with redirects
                        byte[] responseBody = null;

                        if (response.isRedirectCalled()) {
                            // No sending body for redirects (unless set explicitly)
                            if (response.getBody() != null) {
                                responseBody = response.getBody();
                            } else {
                                responseBody = new byte[0];
                            }
                        } else {
                            if (result != null) {
                                // Use result of Route.handle()
                                responseBody = result.toString().getBytes(StandardCharsets.UTF_8);
                            } else if (response.getBody() != null) {
                                // Use body set by body() or bodyAsBytes()
                                responseBody = response.getBody();
                            } else {
                                // No response body to send
                                responseBody = new byte[0];
                            }
                        }

                        int contentLengthResponse = responseBody.length;

                        // Confirm content-length header and body is set
                        response.header("content-length", String.valueOf(contentLengthResponse));
                        response.bodyAsBytes(responseBody);

                        sendResponse(outputStream, response);
                    }
                } catch (Exception e) {
                    // Handle handler exceptions by sending 500 Internal Server Error
                    if(response.isWriteCalled()){
                        logger.error("Exception while handling route, write already called: " + e.getMessage(), e);
                        return;
                    }
                    logger.error("Exception while handling route, write not called: " + e.getMessage(), e);
                    sendErrorResponse(outputStream, 500, "Internal Server Error");
                }

                break; // Break after a single route is already matched
            }
        }

        if (!routeMatched && homeDirectory != null) {
            // No matching route found, check static files like HW1
            Path filePath = Paths.get(homeDirectory + uri);

            // Handle If-Modified-Since header
            if (modifiedSinceHeader != null) {
                Instant modifiedSinceParsed = parseRfc1123Date(modifiedSinceHeader);
                if (Files.exists(filePath)) {
                    Instant lastModifiedTime = Files.getLastModifiedTime(filePath).toInstant();
                    if (modifiedSinceParsed != null && !lastModifiedTime.isAfter(modifiedSinceParsed)) {
                        // Send Not Modified response
                        sendNotModifiedResponse(outputStream);
                        return;
                    }
                }
            }

            // Handle static files - as from HW1
            if (uri.contains("..")) {
                sendErrorResponse(outputStream, 403, "Forbidden");
                return;
            }

            if (!Files.exists(filePath)) {
                sendErrorResponse(outputStream, 404, "Not Found");
                return;
            }

            if (!Files.isReadable(filePath)) {
                sendErrorResponse(outputStream, 403, "Forbidden");
                return;
            }

            String contentType = getContentType(filePath.toString());
            long fileSize = Files.size(filePath);

            // Build response using ResponseImpl
            ResponseImpl response = new ResponseImpl(outputStream);
            response.status(200, "OK");
            response.type(contentType);
            response.header("content-length", String.valueOf(fileSize));

            sendResponse(outputStream, response);

            if (method.equals("GET")) {
                sendFile(outputStream, filePath);
            }

            logger.info("File sent: " + filePath);
        }
    }

    // Helper methods

    private void closeSocket() {
        try {
            clientSocket.close();
            logger.info("Connection closed");
        } catch (IOException e) {
            logger.error("Error closing socket: " + e.getMessage(), e);
        }
    }

    private String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\r') {
                in.mark(1);
                int nextChar = in.read();
                if (nextChar == '\n') {
                    break;
                } else {
                    in.reset();
                    break;
                }
            } else if (c == '\n') {
                break;
            } else {
                sb.append((char) c);
            }
        }

        if (c == -1 && sb.isEmpty()) {
            // End of stream and no data read
            return null;
        }

        return sb.toString();
    }

    private Map<String, String> parseQueryParams(String uri) {
        int queryStart = uri.indexOf('?');
        if (queryStart != -1 && queryStart + 1 < uri.length()) {
            String queryString = uri.substring(queryStart + 1);
            return parseQueryString(queryString);
        }
        return new HashMap<>();
    }

    private Map<String, String> parseQueryString(String queryString) {
        Map<String, String> queryParams = new HashMap<>();
        String[] pairs = queryString.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf('=');
            if (idx != -1) {
                String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
                queryParams.put(key, value);
            } else {
                String key = URLDecoder.decode(pair, StandardCharsets.UTF_8);
                queryParams.put(key, "");
            }
        }
        return queryParams;
    }


    private void sendResponse(OutputStream outputStream, ResponseImpl response) throws IOException {
        if(response.isWriteCalled()){
            return; // Headers and body already sent
        }

        // Use response body
        byte[] responseBody = response.getBody();
        int contentLengthResponse = responseBody != null ? responseBody.length : 0;

        // Confirm content-length header is set
        if (!response.getHeaders().containsKey("content-length")) {
            response.header("content-length", String.valueOf(contentLengthResponse));
        }

        // Write status line
        String statusLine =  "HTTP/1.1 " + response.getStatusCode() + " " + response.getReasonPhrase() + "\r\n";
        outputStream.write(statusLine.getBytes(StandardCharsets.UTF_8));

        // Write headers
        for (Map.Entry<String, List<String>> headerEntry : response.getHeaders().entrySet()) {
            String headerName = headerEntry.getKey();
            for (String headerValue : headerEntry.getValue()) {
                String headerLine = headerName + ": " + headerValue + "\r\n";
                outputStream.write(headerLine.getBytes(StandardCharsets.UTF_8));
            }
        }

        outputStream.write("\r\n".getBytes(StandardCharsets.UTF_8));
        outputStream.flush();

        // Write body
        if (responseBody != null && contentLengthResponse > 0) {
            outputStream.write(responseBody);
            outputStream.flush();
        }
    }

    private void sendErrorResponse(OutputStream outputStream, int statusCode, String statusMessage) throws IOException {
        ResponseImpl response = new ResponseImpl(outputStream);
        response.status(statusCode, statusMessage);
        response.type("text/plain");
        response.body(statusMessage);

        sendResponse(outputStream, response);

        logger.info("Sent error response: " + statusCode + " " + statusMessage);
    }

    private void sendFile(OutputStream outputStream, Path path) throws IOException {
        try (InputStream fileInputStream = Files.newInputStream(path)) {
            byte[] fileBuffer = new byte[1024];
            int read;
            while ((read = fileInputStream.read(fileBuffer)) != -1) {
                outputStream.write(fileBuffer, 0, read);
            }
            outputStream.flush();
        }
    }

    private void sendNotModifiedResponse(OutputStream outputStream) throws IOException {
        ResponseImpl response = new ResponseImpl(outputStream);
        response.status(304, "Not Modified");
        response.type("text/plain");
        response.header("content-length", "0");

        sendResponse(outputStream, response);

        logger.info("Sent 304 Not Modified response");
    }

    private boolean isMethodAllowed(String method) {
        return Arrays.asList(ALLOWED_METHODS).contains(method);
    }

    private boolean isMethodForbidden(String method) {
        return Arrays.asList(FORBIDDEN_METHODS).contains(method);
    }

    private static String getContentType(String filePath) {
        if (filePath.endsWith(".jpg") || filePath.endsWith(".jpeg")) {
            return "image/jpeg";
        } else if (filePath.endsWith(".txt")) {
            return "text/plain";
        } else if (filePath.endsWith(".html")) {
            return "text/html";
        } else {
            return "application/octet-stream";
        }
    }

    private static Instant parseRfc1123Date(String dateStr) {
        try {
            return Instant.from(rfc1123Formatter.parse(dateStr));
        } catch (Exception e) {
            logger.error("Unable to parse If-Modified-Since header: " + dateStr, e);
            return null;
        }
    }
}
