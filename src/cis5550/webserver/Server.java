package cis5550.webserver;

import cis5550.tools.Logger;

import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
//import javax.net.ssl.SSLServerSocketFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Server {
    private static final Logger logger = Logger.getLogger(Server.class);
    private static final int NUM_WORKERS = 100;
    private static int port = 80; // Default Port: 80
    private static int securePort = -1; // Default Value: -1 (invalid port number)
    public static String location = null; // Default Location: null
    public static Server server = null;
    public static boolean isRunning = false;
    private static final List<RouteEntry> routes = new ArrayList<>();
    private static final Map<String, SessionImpl> sessions = new HashMap<>();
    private static final ScheduledExecutorService sessionCleanerExecutor = Executors.newSingleThreadScheduledExecutor();

    // Functions to set port and location
    public static void port(int N) {
        port = N;
    }
    public static void securePort(int N) { securePort = N; }

    public static class staticFiles {
        public static void location(String s) {
            location = s;
        }
    }

    // Functions to make sure server is running, or to launch a thread with it otherwise
    public static Server getServer(){
        if(server == null){
            server = new Server();
        }
        return server;
    }

    private static SSLServerSocketFactory createSSLServerSocketFactory() throws Exception {
        String keyStorePassword = "secret";
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream("keystore.jks"), keyStorePassword.toCharArray());

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        return sslContext.getServerSocketFactory();
    }

    private static void serverLoop(ServerSocket serverSocket, ExecutorService threadPool) {
        try {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept(); // Accept a client connection
                    logger.info("Client connected: " + clientSocket.getInetAddress());

                    try {
                        threadPool.submit(new Worker(clientSocket, location)); // Handle the connection
                    } catch (Exception e) {
                        logger.error("Failed to submit worker task: " + e.getMessage(), e);
                    }
                } catch (IOException ex) {
                    // Handle exceptions thrown by serverSocket.accept() call
                    logger.error("Error accepting client connection: " + ex.getMessage(), ex);
                }
            }
        } catch (Exception ex) {
            // General catch for any other exceptions in server loop.
            logger.error("Unexpected error in server loop: " + ex.getMessage(), ex);
        }
    }

    private static void serverRunning() throws IOException {
        if (!isRunning) {
            isRunning = true;
            new Thread(() -> {
                try {
                    getServer();
                    run();
                } catch (Exception e) {
                    logger.error("Error starting the server: " + e.getMessage(), e);
                }
            }).start();
        }
    }

    // Getter function for stored routes
    public static List<RouteEntry> getRoutes() {
        return routes;
    }

    // Get session, remove session, create session functions
    public static Session getSession(String sessionID) {
        return sessions.getOrDefault(sessionID, null);
    }

    public static void removeSession(String sessionId) {
        sessions.remove(sessionId);
    }

    public static SessionImpl createSession() {
        String newSessionId = generateSessionId();
        SessionImpl newSession = new SessionImpl(newSessionId);
        sessions.put(newSessionId, newSession);
        return newSession;
    }


    // Methods for handling HTTP methods
    public static void get(String path, Route route) throws IOException {
        serverRunning();
        routes.add(new RouteEntry("GET", path, route));
        System.out.println("GET route added for: " + path);
    }

    public static void post(String path, Route route) throws IOException {
        serverRunning();
        routes.add(new RouteEntry("POST", path, route));
        System.out.println("POST route added for: " + path);
    }

    public static void put(String path, Route route) throws IOException {
        serverRunning();
        routes.add(new RouteEntry("PUT", path, route));
        System.out.println("PUT route added for: " + path);
    }


    public static void run() throws IOException {
        // Create thread pool with NUM_WORKERS threads
        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_WORKERS);
        startSessionCleanupTask(); // Every 5 seconds clean up all expired entries

        try {
            // Variables for both HTTP and HTTPS serverSockets and Threads
            ServerSocket serverSocketHTTP;
            Thread httpThread = null;
            ServerSocket serverSocketHTTPS;
            Thread httpsThread = null;


            // Initilize HTTP Socket + Thread and Launch HTTP ServerLoop
            serverSocketHTTP = new ServerSocket(port);
            httpThread = new Thread(() -> serverLoop(serverSocketHTTP, threadPool));
            httpThread.start();
            logger.info("HTTP server started on port " + port);

            // Initilize HTTPS Socket + Thread and Launch HTTPS ServerLoop
            if (securePort >= 0) {
                 SSLServerSocketFactory sslServerSocketFactory = createSSLServerSocketFactory();
                 serverSocketHTTPS = sslServerSocketFactory.createServerSocket(securePort);
//                String pwd = "secret";
//                KeyStore keyStore = KeyStore.getInstance("JKS");
//                keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
//                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
//                keyManagerFactory.init(keyStore, pwd.toCharArray());
//                SSLContext sslContext = SSLContext.getInstance("TLS");
//                sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
//                ServerSocketFactory factory = sslContext.getServerSocketFactory();
//                serverSocketHTTPS = factory.createServerSocket(securePort);

                httpsThread = new Thread(() -> serverLoop(serverSocketHTTPS, threadPool));
                httpsThread.start();
                logger.info("HTTPS server started on port " + securePort);
            }

            // Wait for HTTP/HTTPS threads
            if (httpsThread != null) {
                httpsThread.join();
            }
            httpThread.join();
        } catch (Exception ex) {
            logger.error("Server Exception: " + ex.getMessage(), ex);
        } finally {
            threadPool.shutdown();
        }
    }

    private static String generateSessionId() {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-";
        SecureRandom random = new SecureRandom();
        StringBuilder sb = new StringBuilder(20);
        for (int i = 0; i < 20; i++) {
            int index = random.nextInt(chars.length());
            sb.append(chars.charAt(index));
        }
        return sb.toString();
    }

    private static void startSessionCleanupTask() {
        sessionCleanerExecutor.scheduleAtFixedRate(() -> {
            logger.info("Checking for expired sessions...");
            sessions.entrySet().removeIf(entry -> !entry.getValue().isValid());
        }, 0, 5, TimeUnit.SECONDS);
    }
}