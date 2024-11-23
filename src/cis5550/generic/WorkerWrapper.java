package cis5550.generic;

public class WorkerWrapper {
    private final String id;
    private String ip;
    private int port;
    private long lastPing;

    public WorkerWrapper(String id, String ip, int port, long lastPing) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.lastPing = lastPing;
    }

    public String getId() {
        return id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getLastPing() {
        return lastPing;
    }

    public void updateLastPing(long lastPing) {
        this.lastPing = lastPing;
    }
}
