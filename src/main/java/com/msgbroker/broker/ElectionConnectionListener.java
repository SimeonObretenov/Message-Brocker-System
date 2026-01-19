package com.msgbroker.broker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ThreadFactory;

/**
 * Listens for incoming LEP (Leader Election Protocol) connections
 */
public class ElectionConnectionListener implements Runnable {

    private final ThreadFactory threadFactory = Thread.ofVirtual().factory();
    private final int port;
    private final ElectionManager electionManager;
    private volatile ServerSocket serverSocket;
    private volatile boolean running = true;

    public ElectionConnectionListener(int port, ElectionManager electionManager) {
        this.port = port;
        this.electionManager = electionManager;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            while (running && !serverSocket.isClosed()) {
                try {
                    Socket conn = serverSocket.accept();
                    Thread t = threadFactory.newThread(new ElectionConnectionHandler(conn, electionManager));
                    t.start();
                } catch (IOException e) {
                    if (running) {
                        // Socket accept failed, ignore if shutting down
                    }
                }
            }
        } catch (IOException e) {
            // Could not start server
        }
    }

    public void shutdown() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
    }
}
