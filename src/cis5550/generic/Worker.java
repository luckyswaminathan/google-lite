
package cis5550.generic;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;

public class Worker {

  static protected void startPingThread(Integer workerPort, String coordinatorIP, Integer coordinatorPort,
      String workerId) {
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

          url.getContent();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }).start();
  }

  private static String generateRandomString(int length) {
    String characters = "abcdefghijklmnopqrstuvwxyz";
    Random random = new Random();
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(characters.charAt(random.nextInt(characters.length())));
    }
    return sb.toString();
  }
}
