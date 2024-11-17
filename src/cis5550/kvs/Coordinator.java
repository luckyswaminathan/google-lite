package cis5550.kvs;

import static cis5550.webserver.Server.*;

public class Coordinator extends cis5550.generic.Coordinator {
  public Coordinator() {
    super();
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: java Coordinator <port>");
      System.exit(1);
    }

    int port = Integer.parseInt(args[0]);
    port(port);

    registerRoutes();

    get("/", (req, res) -> {
      String workerTable = workerTable();
      return "<html><head><title>KVS Coordinator</title></head><body>" + workerTable + "</body></html>";
    });
  }
}
