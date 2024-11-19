package cis5550.generic;

import cis5550.tools.HTTP;
import cis5550.tools.Logger;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Worker {
    private static final Logger logger = Logger.getLogger(Worker.class);
    private static final ScheduledExecutorService pingExecutor = Executors.newSingleThreadScheduledExecutor();

    static protected void startPingThread(Integer coordinatorPort, String coordinatorIP, Integer workerPort, String workerId) {
        new Thread(() -> {
            try {
                while (true) {
                    // Wait for the required interval (e.g., 5000 milliseconds)
                    Thread.sleep(5000);

                    // Generate a random string of five lower-case letters
                    // String randomString = generateRandomString(5);

                    // Create the URL
                    String urlString = String.format("http://%s:%d/ping?id=%s&port=%d", coordinatorIP, coordinatorPort,
                            workerId, workerPort);
                    URL url = new URL(urlString);
                    System.out.println("Calling first function");
                    url.getContent();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    public static void startPingThread(int portNum, String storageDir, String coordinatorIpPort) {
        pingExecutor.scheduleAtFixedRate(() -> {
            String workerId = getIdForWorker(storageDir);
            try {
                URL callURL = new URL("http://" + coordinatorIpPort + "/ping?id=" + workerId + "&port=" + portNum);
                System.out.println("Calling second function");
                callURL.getContent();
            } catch (MalformedURLException e) {
                logger.error("Error, invalid coordinatorIpPort, id, or worker port format" + e.getMessage(), e);
            } catch (IOException e) {
                logger.error("Error Pinging " + coordinatorIpPort + " for worker " + workerId, e);
            }
//            logger.info("Pinged " + coordinatorIpPort + " for worker " + workerId);
        }, 5, 5, TimeUnit.SECONDS);
    }

    public static void startPingThread(String coordinatorIpPort, String workerId, int workerPort) {
        pingExecutor.scheduleAtFixedRate(() -> {
            try {
                URL callURL = new URL("http://" + coordinatorIpPort + "/ping?id=" + workerId + "&port=" + workerPort);
                System.out.println("Calling third function");
                callURL.getContent();
            } catch (MalformedURLException e) {
                logger.error("Error, invalid coordinatorIpPort, id, or worker port format" + e.getMessage(), e);
            } catch (IOException e) {
                logger.error("Error Pinging " + coordinatorIpPort + " for worker " + workerId, e);
            }
//            logger.info("Pinged " + coordinatorIpPort + " for worker " + workerId);
        }, 5, 5, TimeUnit.SECONDS);
    }

    public static String getIdForWorker(String storageDir){
        File idStore = new File(storageDir, "id");
        String workerId = null;

        if (idStore.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(idStore))){
                workerId = reader.readLine();
            } catch (IOException e) {
                logger.error("Error reading ID from file: " + e.getMessage(), e);
                System.exit(1);
            }
        } else {
            workerId = generateRandomIdForWorker();
            try (FileWriter writer = new FileWriter(idStore)) {
                writer.write(workerId);
            } catch (IOException e) {
                logger.error("Error writing ID to file: " + e.getMessage(), e);
            }
        }

        return workerId;
    }

    private static String generateRandomIdForWorker() {
        Random random = new Random();
        StringBuilder retVal = new StringBuilder(5);
        String letters = "abcdefghijklmnopqrstuvwxyz";
        for (int i = 0; i < 5; i++) {
            retVal.append(letters.charAt(random.nextInt(letters.length())));
        }
        return retVal.toString();
    }
}
