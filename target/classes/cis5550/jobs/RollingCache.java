package cis5550.jobs;

import java.util.LinkedHashMap;
import java.util.Map;

public class RollingCache {
    private final int MAX_ENTRIES = 10_000;  // Adjust based on memory constraints
    private LinkedHashMap<String, Boolean> cache;

    public RollingCache() {
        cache = new LinkedHashMap<String, Boolean>(MAX_ENTRIES + 1, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > MAX_ENTRIES;
            }
        };
    }

    public boolean checkAndAdd(String path) {
        String domainHash = path.substring(0, Math.min(path.length(), 8));
        return cache.putIfAbsent(domainHash, Boolean.TRUE) != null;
    }
}