package com.msgbroker.broker;

import com.msgbroker.ComponentFactory;
import com.msgbroker.config.BrokerConfig;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.ThreadFactory;

public class Broker implements IBroker {

    private final ThreadFactory threadFactory = Thread.ofVirtual().factory();
    private final BrokerConfig config;
    private final BrokerState brokerState;
    private BrokerConnectionListener listener;
    private Thread listenerThread;
    private DNSRegistrationClient dnsClient;
    private DatagramSocket monitoringSocket;
    
    // Election components
    private ElectionManager electionManager;
    private ElectionConnectionListener electionListener;
    private Thread electionListenerThread;

    public Broker(BrokerConfig config) {
        this.config = config;
        this.brokerState = new BrokerState();
    }

    @Override
    public void run() {
        // Initialize DNS client first (used by election manager)
        dnsClient = new DNSRegistrationClient(config.dnsHost(), config.dnsPort());
        
        // Initialize monitoring UDP socket
        try {
            monitoringSocket = new DatagramSocket();
        } catch (IOException e) {
            // Continue without monitoring if socket creation fails
        }
        
        // Initialize election manager
        electionManager = new ElectionManager(config, dnsClient);
        
        // Start the election listener if election type is configured
        if (config.electionType() != null && !config.electionType().equals("none")) {
            electionListener = new ElectionConnectionListener(config.electionPort(), electionManager);
            electionListenerThread = threadFactory.newThread(electionListener);
            electionListenerThread.start();
            
            // Start heartbeat timeout monitoring
            electionManager.startHeartbeatTimeoutMonitor();
        }
        
        // Start the broker listener (SMQP)
        listener = new BrokerConnectionListener(config.port(), brokerState, this::sendMonitoringData);
        listenerThread = threadFactory.newThread(listener);
        listenerThread.start();

        // Register with DNS server (SMQP domain)
        String address = config.host() + ":" + config.port();
        dnsClient.registerDomain(config.domain(), address);

        // Wait for listener to finish
        try {
            listenerThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void shutdown() {
        // Shutdown election manager first
        if (electionManager != null) {
            electionManager.shutdown();
        }
        
        // Shutdown election listener
        if (electionListener != null) {
            electionListener.shutdown();
        }
        if (electionListenerThread != null) {
            electionListenerThread.interrupt();
        }
        
        // Unregister from DNS
        if (dnsClient != null && config.domain() != null) {
            dnsClient.unregisterDomain(config.domain());
        }

        // Shutdown listener
        if (listener != null) {
            listener.shutdown();
        }
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
        
        // Close monitoring socket
        if (monitoringSocket != null && !monitoringSocket.isClosed()) {
            monitoringSocket.close();
        }
    }
    
    /**
     * Sends monitoring data to the UDP monitoring server.
     * Format: <broker-ip>:<broker-port> <routing-key>
     */
    private void sendMonitoringData(String routingKey) {
        if (monitoringSocket == null || config.monitoringHost() == null) {
            return;
        }
        
        try {
            String message = config.host() + ":" + config.port() + " " + routingKey;
            byte[] data = message.getBytes();
            InetAddress address = InetAddress.getByName(config.monitoringHost());
            DatagramPacket packet = new DatagramPacket(data, data.length, address, config.monitoringPort());
            monitoringSocket.send(packet);
        } catch (IOException e) {
            // Silently ignore monitoring failures
        }
    }

    public static void main(String[] args) {
        ComponentFactory.createBroker(args[0]).run();
    }

    @Override
    public int getId() {
        return config.electionId();
    }

    @Override
    public void initiateElection() {
        if (electionManager != null) {
            electionManager.initiateElection();
        }
    }

    @Override
    public int getLeader() {
        if (electionManager != null) {
            return electionManager.getLeader();
        }
        return -1;
    }
}
