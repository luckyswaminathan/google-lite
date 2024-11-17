package cis5550.generic;

import java.lang.reflect.Array;
import java.time.LocalTime;
import java.util.ArrayList;
import static cis5550.webserver.Server.*;

public class Coordinator {

  static ArrayList<WorkerData> workers = new ArrayList<WorkerData>();

  static ArrayList<String> getWorkers() {
    return new ArrayList<String>();
  }

  protected static String workerTable() {
    StringBuilder html = new StringBuilder();
    html.append("<html><body><table border='1'><tr><th>ID</th><th>IP</th><th>Port</th></tr>");
    synchronized (workers) {
      for (WorkerData worker : workers) {
        html.append("<tr>")
            .append("<td>").append(worker.getWorkerId()).append("</td>")
            .append("<td>").append(worker.getIp()).append("</td>")
            .append("<td><a href='http://").append(worker.getIp()).append(":").append(worker.getPort()).append("'>")
            .append(worker.getPort()).append("</a></td>")
            .append("</tr>");
      }
    }
    html.append("</table></body></html>");
    return html.toString();
  }

  protected static void registerRoutes() {
    get("/ping", (req, res) -> {
      String workerId = req.queryParams("id");
      String port = req.queryParams("port");
      String ip = req.ip();
      System.out.println("Received ping from worker " + workerId + " on port " + port);

      if (workerId != null && port != null) {
        synchronized (workers) {
          boolean found = false;
          for (WorkerData worker : workers) {
            if (worker.getWorkerId().equals(workerId)) {
              worker.setLastPing(LocalTime.now());
              found = true;
              break;
            }
          }
          if (!found) {
            workers.add(new WorkerData(workerId, port, ip));
          }
        }
        return "OK";
      } else {
        res.status(400, "Missing id or port parameter");
        return "Missing id or port parameter";
      }
    });

    get("/workers", (req, res) -> {
      StringBuilder response = new StringBuilder();
      synchronized (workers) {
        workers.removeIf(worker -> worker.getLastPing().isBefore(LocalTime.now().minusSeconds(15))); // Remove inactive
                                                                                                     // workers
        response.append(workers.size()).append("\n");
        for (WorkerData worker : workers) {
          response.append(worker.getWorkerId()).append(",").append(worker.getIp()).append(":").append(worker.getPort())
              .append("\n");
        }
      }
      return response.toString();
    });
  }
}
