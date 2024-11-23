package cis5550.webserver;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ResponseImpl implements Response {

    private int statusCode = 200;
    private String reasonPhrase = "OK";
    private final Map<String, List<String>> headers;
    private byte[] body;
    private boolean writeCalled = false;
    private boolean headersSent = false;
    private boolean redirectCalled = false;

    // Add OutputStream to write directly to the connection
    private final OutputStream outputStream;

    public ResponseImpl(OutputStream outputStream) {
        this.outputStream = outputStream;
        this.headers = new LinkedHashMap<>();
        this.type("text/html");
    }

    @Override
    public void body(String body) {
        if (!writeCalled && !redirectCalled) {
            if (body != null) {
                this.body = body.getBytes(StandardCharsets.UTF_8);
            } else {
                this.body = null;
            }
        }
    }

    @Override
    public void bodyAsBytes(byte[] bodyArg) {
        if (!writeCalled && !redirectCalled) {
            this.body = bodyArg;
        }
    }

    @Override
    public void header(String name, String value) {
        if (!writeCalled && !redirectCalled) {
            headers.computeIfAbsent(name, k -> new ArrayList<>()).add(value.toLowerCase());
        }
    }

    @Override
    public void type(String contentType) {
        if (!writeCalled && !redirectCalled) {
            headers.remove("Content-Type");
            headers.remove("content-type");
            header("Content-Type", contentType);
        }
    }

    @Override
    public void status(int statusCode, String reasonPhrase) {
        if (!writeCalled && !redirectCalled) {
            this.statusCode = statusCode;
            this.reasonPhrase = reasonPhrase;
        }
    }

    @Override
    public void write(byte[] b) throws Exception {
        if (!writeCalled && !redirectCalled) {
            writeCalled = true;
//            headers.remove("Content-Length");
//            headers.remove("content-length");
            System.out.println(Arrays.toString(headers.entrySet().toArray()));
            header("Connection", "close");
            sendHeaders();
        }
        outputStream.write(b);
        outputStream.flush();
    }

    private void sendHeaders() throws IOException {
        if (!headersSent) {
            // Write status line
            String statusLine = "HTTP/1.1 " + statusCode + " " + reasonPhrase + "\r\n";
            outputStream.write(statusLine.getBytes(StandardCharsets.UTF_8));

            // Write headers
            for (Map.Entry<String, List<String>> headerEntry : headers.entrySet()) {
                String headerName = headerEntry.getKey();
                for (String headerValue : headerEntry.getValue()) {
                    String headerLine = headerName + ": " + headerValue + "\r\n";
                    outputStream.write(headerLine.getBytes(StandardCharsets.UTF_8));
                }
            }

            outputStream.write("\r\n".getBytes(StandardCharsets.UTF_8));
            outputStream.flush();

            headersSent = true;
        }
    }

    @Override
    public void redirect(String url, int responseCode) {
        if (writeCalled) {
            // Cannot redirect after response committed
            return;
        }

        // Only allow valid redirect status codes
        if (responseCode != 301 && responseCode != 302 && responseCode != 303 &&
                responseCode != 307 && responseCode != 308) {
            throw new IllegalArgumentException("Invalid redirect status code: " + responseCode);
        }

        this.statusCode = responseCode;
        this.reasonPhrase = getReasonPhraseForRedirectStatusCode(responseCode);
        header("location", url);
        redirectCalled = true;
    }

    // Found code meanings here: https://www.drlinkcheck.com/blog/http-redirects-301-302-303-307-308
    private String getReasonPhraseForRedirectStatusCode(int statusCode) {
        return switch (statusCode) {
            case 301 -> "Permanently Moved";
            case 302 -> "Found But Temporary Redirect";
            case 303 -> "See Other";
            case 307 -> "Temporary Redirect";
            case 308 -> "Permanent Redirect";
            default -> "Redirect";
        };
    }

    @Override
    public void halt(int statusCode, String reasonPhrase) {
        // Dummy method
    }

    // Getters
    public int getStatusCode() {
        return statusCode;
    }

    public String getReasonPhrase() {
        return reasonPhrase;
    }

    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    public byte[] getBody() {
        return body;
    }

    public boolean isWriteCalled() {
        return writeCalled;
    }

    public boolean isRedirectCalled() {
        return redirectCalled;
    }
}