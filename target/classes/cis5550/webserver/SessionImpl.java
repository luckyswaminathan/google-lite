package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;

public class SessionImpl implements Session {
    private final String sessionId;
    private final long creationTime;
    private long lastAccessedTime;
    private int maxActiveInterval = 300; // in seconds
    private final Map<String, Object> attributes;
    private boolean isValid;

    public SessionImpl(String sessionId) {
        this.sessionId = sessionId;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessedTime = this.creationTime;
        this.attributes = new HashMap<>();
        this.isValid = true;
    }

    // Getters and Setters
    public String id() { return sessionId; }
    public long creationTime() { return creationTime; }
    public long lastAccessedTime() { return lastAccessedTime; }
    public void maxActiveInterval(int seconds) { this.maxActiveInterval = seconds; }
    public void updateAccessTime() { this.lastAccessedTime = System.currentTimeMillis(); }

    // Invalidate by setting isValid false, clearing attributes, and removing from server sessions
    public void invalidate() {
        this.isValid = false;
        this.attributes.clear();
    }

    // Attribute getter and setter functions
    public Object attribute(String name) {
        return this.isValid ? attributes.get(name) : null;
    }

    public void attribute(String name, Object value) {
        if (this.isValid) {
            attributes.put(name, value);
        }
    }

    // Check if session is valid using max active interval.
    public boolean isValid() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastAccessedTime > (maxActiveInterval * 1000L)) {
            invalidate();
        }
        return this.isValid;
    }
}
