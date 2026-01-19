package com.msgbroker.dns;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

public class DNSConnectionListener implements Runnable {

    private final ThreadFactory threadFactory = Thread.ofVirtual().factory();
    private final int port;
    private final ConcurrentHashMap<String, String> domainRegistry;
    private volatile ServerSocket serverSocket;
    private volatile boolean running = true;

    public DNSConnectionListener(int port, ConcurrentHashMap<String, String> domainRegistry) {
        this.port = port;
        this.domainRegistry = domainRegistry;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            while (running && !serverSocket.isClosed()) {
                try {
                    Socket conn = serverSocket.accept();
                    Thread t = threadFactory.newThread(new DNSConnectionHandler(conn, domainRegistry));
                    t.start();
                } catch (IOException e) {
                    if (running) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
