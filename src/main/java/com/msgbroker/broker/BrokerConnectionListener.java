package com.msgbroker.broker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

public class BrokerConnectionListener implements Runnable {

    private final ThreadFactory threadFactory = Thread.ofVirtual().factory();
    private final int port;
    private final BrokerState brokerState;
    private final Consumer<String> monitoringCallback;
    private volatile ServerSocket serverSocket;
    private volatile boolean running = true;

    public BrokerConnectionListener(int port, BrokerState brokerState, Consumer<String> monitoringCallback) {
        this.port = port;
        this.brokerState = brokerState;
        this.monitoringCallback = monitoringCallback;
    }

    @Override
    public void run() {
        try {
            serverSocket = new ServerSocket(port);
            while (running && !serverSocket.isClosed()) {
                try {
                    Socket conn = serverSocket.accept();
                    Thread t = threadFactory.newThread(new BrokerConnectionHandler(conn, brokerState, monitoringCallback));
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
