package com.msgbroker.monitoring;

import com.msgbroker.ComponentFactory;
import com.msgbroker.config.MonitoringServerConfig;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MonitoringServer implements IMonitoringServer {

    private final MonitoringServerConfig config;
    private DatagramSocket socket;
    private volatile boolean running = true;
    
    // Statistics: Server (ip:port) -> (routingKey -> count)
    private final Map<String, Map<String, AtomicInteger>> statistics = new ConcurrentHashMap<>();
    private final AtomicInteger totalMessages = new AtomicInteger(0);

    public MonitoringServer(MonitoringServerConfig config) {
        this.config = config;
    }

    @Override
    public void run() {
        try {
            socket = new DatagramSocket(config.monitoringPort());
            byte[] buffer = new byte[1024];
            
            while (running) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength()).trim();
                    processMessage(message);
                } catch (SocketException e) {
                    // Socket closed during shutdown
                    if (running) {
                        e.printStackTrace();
                    }
                } catch (IOException e) {
                    if (running) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
    }
    
    private void processMessage(String message) {
        // Expected format: <ip>:<port> <routing-key>
        String[] parts = message.split("\\s+", 2);
        if (parts.length != 2) {
            return; // Invalid message format, discard
        }
        
        String server = parts[0]; // ip:port
        String routingKey = parts[1];
        
        // Validate server format (should contain :)
        if (!server.contains(":")) {
            return; // Invalid format, discard
        }
        
        // Update statistics
        statistics.computeIfAbsent(server, k -> new ConcurrentHashMap<>())
                  .computeIfAbsent(routingKey, k -> new AtomicInteger(0))
                  .incrementAndGet();
        
        totalMessages.incrementAndGet();
    }

    @Override
    public void shutdown() {
        running = false;
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    @Override
    public int receivedMessages() {
        return totalMessages.get();
    }

    @Override
    public String getStatistics() {
        StringBuilder sb = new StringBuilder();
        
        for (Map.Entry<String, Map<String, AtomicInteger>> serverEntry : statistics.entrySet()) {
            sb.append("Server ").append(serverEntry.getKey()).append("\n");
            
            for (Map.Entry<String, AtomicInteger> keyEntry : serverEntry.getValue().entrySet()) {
                sb.append("  ").append(keyEntry.getKey()).append(" ").append(keyEntry.getValue().get()).append("\n");
            }
        }
        
        // Remove trailing newline if present
        if (!sb.isEmpty() && sb.charAt(sb.length() - 1) == '\n') {
            sb.setLength(sb.length() - 1);
        }
        
        return sb.toString();
    }

    public static void main(String[] args) {
        ComponentFactory.createMonitoringServer(args[0]).run();
    }
}
