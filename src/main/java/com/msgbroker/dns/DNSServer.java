package com.msgbroker.dns;

import com.msgbroker.ComponentFactory;
import com.msgbroker.config.DNSServerConfig;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

public class DNSServer implements IDNSServer {

    // virutal threads(cheaper than platform threads) 
    private final ThreadFactory threadFactory = Thread.ofVirtual().factory();
    private final DNSServerConfig config;
    //shared state(multiple threads can read/write without synchronization)
    private final ConcurrentHashMap<String, String> domainRegistry;
    private DNSConnectionListener listener;
    private Thread listenerThread;

    public DNSServer(DNSServerConfig config) {
        this.config = config;
        this.domainRegistry = new ConcurrentHashMap<>();
    }

    @Override
    public void shutdown() {
        if (listener != null) {
            listener.shutdown(); // stops accepting new connections
        }
        if (listenerThread != null) {
            listenerThread.interrupt(); // wake up the listener thread if it's blocked on accept
        }
    }

    // creates listener and starts it in a virtual thread
    @Override
    public void run() {
        listener = new DNSConnectionListener(config.port(), domainRegistry);
        listenerThread = threadFactory.newThread(listener);
        listenerThread.start();
        try {
            listenerThread.join(); // blocks on to prevent premature exit
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        ComponentFactory.createDNSServer(args[0]).run();
    }
}
