package cis5550.generic;

import java.time.LocalTime;

public class WorkerData {
  private String workerId;
  private String port;
  private LocalTime lastPing;
  private String ip;

  public WorkerData(String workerId, String port, String ip) {
    this.workerId = workerId;
    this.port = port;
    this.ip = ip;
    this.lastPing = LocalTime.now();
  }

  public String getWorkerId() {
    return workerId;
  }

  public String getPort() {
    return port;
  }

  public String getIp() {
    return ip;
  }

  public LocalTime getLastPing() {
    return lastPing;
  }

  public void setLastPing(LocalTime theLastPing) {
    this.lastPing = theLastPing;
  }
}
