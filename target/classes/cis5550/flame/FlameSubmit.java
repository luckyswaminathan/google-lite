package cis5550.flame;

import java.net.*;
import java.io.*;

public class FlameSubmit {

  static int responseCode;
  static String errorResponse;
  private static final int CONNECT_TIMEOUT = 30000; // 30 seconds
  private static final int READ_TIMEOUT = 30000;    // 30 seconds
  private static final int MAX_RETRIES = 3;

  public static String submit(String server, String jarFileName, String className, String arg[]) throws Exception {
    responseCode = 200;
    errorResponse = null;

    Exception lastException = null;
    for (int retry = 0; retry < MAX_RETRIES; retry++) {
      try {
        String u = "http://"+server+"/submit"+"?class="+className;
        for (int i=0; i<arg.length; i++)
          u = u + "&arg"+(i+1)+"="+URLEncoder.encode(arg[i], "UTF-8");

        File f = new File(jarFileName);
        byte jarFile[] = new byte[(int)f.length()];
        FileInputStream fis = new FileInputStream(jarFileName);
        fis.read(jarFile);
        fis.close();

        System.out.println(u);

        HttpURLConnection con = (HttpURLConnection)(new URI(u).toURL()).openConnection();
        con.setConnectTimeout(CONNECT_TIMEOUT);
        con.setReadTimeout(READ_TIMEOUT);
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setFixedLengthStreamingMode(jarFile.length);
        con.setRequestProperty("Content-Type", "application/jar-archive");
        con.connect();

        try (OutputStream out = con.getOutputStream()) {
          out.write(jarFile);
        }

        try {
          BufferedReader r = new BufferedReader(new InputStreamReader(con.getInputStream()));
          StringBuilder result = new StringBuilder();
          String line;
          while ((line = r.readLine()) != null) {
            if (result.length() > 0) result.append("\n");
            result.append(line);
          }
          return result.toString();

        } catch (IOException ioe) {
          responseCode = con.getResponseCode();
          try (BufferedReader r = new BufferedReader(new InputStreamReader(con.getErrorStream()))) {
            StringBuilder error = new StringBuilder();
            String line;
            while ((line = r.readLine()) != null) {
              if (error.length() > 0) error.append("\n");
              error.append(line);
            }
            errorResponse = error.toString();
          }
          return null;
        }

      } catch (ConnectException | SocketTimeoutException e) {
        lastException = e;
        if (retry < MAX_RETRIES - 1) {
          // Wait before retrying, using exponential backoff
          Thread.sleep((retry + 1) * 2000L);
          System.err.println("[Retry " + (retry + 1) + "/" + MAX_RETRIES + "] Connection failed, retrying...");
          continue;
        }
      }
    }

    // If we got here, all retries failed
    throw new Exception("Failed to connect after " + MAX_RETRIES + " attempts", lastException);
  }

  public static int getResponseCode() {
    return responseCode;
  }

  public static String getErrorResponse() {
    return errorResponse;
  }

  public static void main(String args[]) {
    if (args.length < 3) {
      System.err.println("Syntax: FlameSubmit <server> <jarFile> <className> [args...]");
      System.exit(1);
    }

    for (int i = 0; i < args.length; i++) {
      System.err.println("Argument " + i + ": " + args[i]);
    }

    String[] arg = new String[args.length-3];
    for (int i=3; i<args.length; i++)
      arg[i-3] = args[i];

    try {
      String response = submit(args[0], args[1], args[2], arg);
      if (response != null) {
        System.out.println(response);
      } else {
        System.err.println("*** JOB FAILED ***\n");
        System.err.println(getErrorResponse());
      }
    } catch (Exception e) {
      System.err.println("*** SUBMISSION FAILED ***");
      e.printStackTrace();
    }
  }
}