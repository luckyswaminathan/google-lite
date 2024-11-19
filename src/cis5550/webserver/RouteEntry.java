package cis5550.webserver;

import java.util.*;

// Record class since data is immutable, just carrying paths and have helper function
public record RouteEntry(String method, String pathPattern, Route handler) {

    // Method to check if the URI matches the path pattern
    public boolean matches(String uri) {
        return matchAndExtractParams(uri) != null;
    }

    // Method to extract path parameters
    public Map<String, String> extractParams(String uri) {
        return matchAndExtractParams(uri);
    }

    // Helper Method for pattern matching
    private Map<String, String> matchAndExtractParams(String uri) {
        // Split pathPattern and URI into parts - ignore query params
        String[] patternParts = pathPattern.split("/");
        String[] uriParts = uri.split("\\?")[0].split("/");

        // Remove leading empty parts from leading slashes
        patternParts = trimLeadingEmptyParts(patternParts);
        uriParts = trimLeadingEmptyParts(uriParts);

        // If not match, not a valid pattern (return null)
        if (patternParts.length != uriParts.length) {
            return null;
        }

        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < patternParts.length; i++) {
            String patternPart = patternParts[i];
            String uriPart = uriParts[i];

            if (patternPart.startsWith(":")) {
                // Extract parameter name and corresponding value from URI
                String paramName = patternPart.substring(1);
                params.put(paramName, uriPart);
            } else if (!patternPart.equals(uriPart)) {
                // If a non-parameter part doesn't match, return null
                return null;
            }
            // If it gets here, then either this patternPart is either a path param or it's an exact match, otherwise would've returned null
        }
        return params;
    }

    // Helper method to remove leading empty parts
    private String[] trimLeadingEmptyParts(String[] parts) {
        if (parts.length > 0 && parts[0].isEmpty()) {
            return Arrays.copyOfRange(parts, 1, parts.length);
        }
        return parts;
    }

}
