package cis5550.generic;


import cis5550.tools.Logger;

import java.io.IOException;
import java.util.*;

import static cis5550.webserver.Server.*;

public class Coordinator {
    private static final Map<String, WorkerWrapper> workers = new HashMap<>();
    private static final Logger logger = Logger.getLogger(Coordinator.class);

    public static Vector<String> getWorkers() {
        cleanUpWorkers();

        Vector<String> workersToReturn = new Vector<>();
        for (WorkerWrapper workerWrapper : workers.values()) {
            workersToReturn.add(workerWrapper.getIp() + ":" + workerWrapper.getPort());
        }
        return workersToReturn;
    }

    public static String workerTable() {
        cleanUpWorkers();

        StringBuilder html = new StringBuilder();
        html.append("<table border='1'>");
        html.append("<tr><th>ID</th><th>IP:Port</th><th>Hyperlink</th></tr>");
        // Construct a row for each worker
        for (WorkerWrapper workerWrapper : workers.values()) {
            html.append("<tr>");
            // Add ID to row
            html.append("<td>").append(workerWrapper.getId()).append("</td>");

            // Add IP:Port string to row
            html.append("<td>").append(workerWrapper.getIp()).append(":").append(workerWrapper.getPort()).append("</td>");

            // Add Hyperlink string to row
            html.append("<td>");
            html.append("<a href='http://").append(workerWrapper.getIp()).append(":").append(workerWrapper.getPort()).append("/'>")
                    .append("http://").append(workerWrapper.getIp()).append(":").append(workerWrapper.getPort()).append("/")
                    .append("</a>");
            html.append("</td>");

            html.append("</tr>");
        }
        html.append("</table>");
        return html.toString();
    }

    public static void registerRoutes() throws IOException {
        // Create routes for /ping and /workers routes
        get("/ping", (req, res) -> {
            res.type("text/plain");
            String ip = req.ip();
            String id = req.queryParams("id");
            String portStr = req.queryParams("port");

            logger.info("Received /ping from IP: " + ip + " with id: " + id + " and port: " + portStr);

            if (id == null || portStr == null) {
                res.status(400, "Bad Request");
                logger.warn("Missing id or port in /ping request from IP: " + ip);
                return "Missing id or port";
            }

            int port = Integer.parseInt(portStr);
            long currTime = System.currentTimeMillis();

            WorkerWrapper workerWrapper = workers.get(id);
            if (workerWrapper == null){
                workerWrapper = new WorkerWrapper(id, ip, port, currTime);
                workers.put(id, workerWrapper);
                logger.info("Added new worker: " + id + " at " + ip + ":" + port);
            } else {
                workerWrapper.setIp(ip);
                workerWrapper.setPort(port);
                workerWrapper.updateLastPing(currTime);
                logger.info("Updated worker: " + id + " at " + ip + ":" + port);
            }

            return "OK";
        });

        get("/workers", (req, res) -> {
            cleanUpWorkers();
            logger.info("Received request for /workers");

            StringBuilder retVal = new StringBuilder();
            retVal.append(workers.size()).append('\n');
            for(WorkerWrapper workerWrapper : workers.values()) {
                retVal.append(workerWrapper.getId()).append(",").append(workerWrapper.getIp()).append(":").append(workerWrapper.getPort()).append('\n');
            }
            return retVal.toString();
        });
    }

    private static void cleanUpWorkers() {
        long currentTime = System.currentTimeMillis();
        long timeout = 15 * 1000;

        int initialSize = workers.size();
        workers.entrySet().removeIf(entry -> (currentTime - entry.getValue().getLastPing()) > timeout);
        int finalSize = workers.size();

        if (finalSize < initialSize) {
            logger.info("Cleaned up workers. Removed " + (initialSize - finalSize) + " inactive workers.");
        } else {
            logger.info("Cleaned up workers. No workers removed.");
        }
    }

}
