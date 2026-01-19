package com.msgbroker.broker;

import com.msgbroker.broker.enums.ElectionState;
import com.msgbroker.config.BrokerConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the leader election process for a Message Broker.
 * Supports Ring, Bully, and Raft election algorithms.
 */
public class ElectionManager {

    private final BrokerConfig config;
    private final AtomicReference<ElectionState> state = new AtomicReference<>(ElectionState.CANDIDATE);
    private final AtomicInteger leaderId = new AtomicInteger(-1);
    private final DNSRegistrationClient dnsClient;
    
    // Heartbeat tracking
    private volatile long lastHeartbeatTime;
    private volatile boolean running = true;
    private Thread heartbeatThread;
    private Thread heartbeatTimeoutThread;
    
    // Raft voting
    private final AtomicInteger votedFor = new AtomicInteger(-1);
    private final AtomicInteger votesReceived = new AtomicInteger(0);
    private final Map<Integer, Socket> peerConnections = new ConcurrentHashMap<>();

    public ElectionManager(BrokerConfig config, DNSRegistrationClient dnsClient) {
        this.config = config;
        this.dnsClient = dnsClient;
        this.lastHeartbeatTime = System.currentTimeMillis();
    }

    public int getId() {
        return config.electionId();
    }

    public int getLeader() {
        return leaderId.get();
    }

    public ElectionState getState() {
        return state.get();
    }

    public void setState(ElectionState newState) {
        state.set(newState);
    }
    
    /**
     * Set leader without starting heartbeats (used when we receive declare)
     */
    private void setLeaderState(int id) {
        leaderId.set(id);
        if (id == config.electionId()) {
            state.set(ElectionState.LEADER);
            registerAsDnsLeader();
        } else if (id != -1) {
            state.set(ElectionState.FOLLOWER);
            resetHeartbeatTimer();
        }
    }
    
    /**
     * Become leader and start heartbeats (used after declare has been sent)
     */
    private void becomeLeaderAndStartHeartbeats() {
        leaderId.set(config.electionId());
        state.set(ElectionState.LEADER);
        registerAsDnsLeader();
        startHeartbeatSender();
    }

    /**
     * Initiates an election based on the configured election type.
     */
    public void initiateElection() {
        String electionType = config.electionType();
        if (electionType == null || electionType.equals("none")) {
            return;
        }

        state.set(ElectionState.CANDIDATE);
        lastHeartbeatTime = System.currentTimeMillis();

        switch (electionType) {
            case "ring" -> initiateRingElection();
            case "bully" -> initiateBullyElection();
            case "raft" -> initiateRaftElection();
        }
    }

    /**
     * Ring Election: Send elect message to successor (first peer)
     */
    private void initiateRingElection() {
        sendToPeerAsync(0, "elect " + config.electionId());
    }

    /**
     * Bully Election: Send elect to all peers with higher IDs
     */
    private void initiateBullyElection() {
        boolean gotResponse = false;
        
        for (int i = 0; i < config.electionPeerIds().length; i++) {
            if (config.electionPeerIds()[i] > config.electionId()) {
                String response = sendToPeerAndGetResponse(i, "elect " + config.electionId());
                if (response != null && response.equals("ok")) {
                    gotResponse = true;
                }
            }
        }
        
        // If no one with higher ID responded, we are the leader
        if (!gotResponse) {
            declareAsLeader();
        }
    }

    /**
     * Raft Election: Send elect to all peers requesting votes
     */
    private void initiateRaftElection() {
        votesReceived.set(1); // Vote for ourselves
        votedFor.set(config.electionId());
        
        int numPeers = config.electionPeerIds().length;
        
        // Send elect to all peers and collect votes
        for (int i = 0; i < numPeers; i++) {
            final int peerIndex = i;
            String response = sendToPeerAndGetResponse(peerIndex, "elect " + config.electionId());
            
            // Parse vote response: "vote <sender-id> <candidate-id>"
            if (response != null && response.startsWith("vote")) {
                String[] parts = response.split("\\s+");
                if (parts.length == 3) {
                    try {
                        int candidateId = Integer.parseInt(parts[2]);
                        if (candidateId == config.electionId()) {
                            votesReceived.incrementAndGet();
                        }
                    } catch (NumberFormatException e) {
                        // Invalid vote, ignore
                    }
                }
            }
        }
        
        // Check if we have majority
        int majority = (numPeers + 1) / 2 + 1;
        if (votesReceived.get() >= majority && state.get() == ElectionState.CANDIDATE) {
            declareAsLeader();
        }
    }

    /**
     * Handle incoming elect command
     */
    public String handleElect(int receivedId, PrintWriter responseOut) {
        String electionType = config.electionType();
        
        switch (electionType) {
            case "ring" -> {
                return handleRingElect(receivedId);
            }
            case "bully" -> {
                return handleBullyElect(receivedId);
            }
            case "raft" -> {
                return handleRaftElect(receivedId);
            }
            default -> {
                return "ok";
            }
        }
    }

    private String handleRingElect(int receivedId) {
        if (receivedId == config.electionId()) {
            // My ID came back around - I am the leader
            // First set leader state (without heartbeats)
            setLeaderState(config.electionId());
            // Do the rest asynchronously so we can respond "ok" immediately
            Thread.ofVirtual().start(() -> {
                // Send declare FIRST, then start heartbeats
                sendToPeerSync(0, "declare " + config.electionId());
                // Now start heartbeats
                startHeartbeatSender();
            });
        } else if (receivedId > config.electionId()) {
            // Forward the higher ID
            sendToPeerAsync(0, "elect " + receivedId);
        } else {
            // Forward my higher ID
            sendToPeerAsync(0, "elect " + config.electionId());
        }
        return "ok";
    }

    private String handleBullyElect(int receivedId) {
        // In Bully, we respond OK to acknowledge and may start our own election
        if (config.electionId() > receivedId) {
            // We have higher ID, start our own election to higher nodes
            Thread.ofVirtual().start(this::initiateBullyElection);
        }
        return "ok";
    }

    private String handleRaftElect(int receivedId) {
        state.set(ElectionState.CANDIDATE);
        
        // Vote for candidate if we haven't voted yet this term
        int currentVote = votedFor.get();
        if (currentVote == -1 || currentVote == receivedId) {
            votedFor.set(receivedId);
            return "vote " + config.electionId() + " " + receivedId;
        } else {
            // Already voted for someone else this term
            return "vote " + config.electionId() + " " + currentVote;
        }
    }

    /**
     * Handle incoming declare command
     */
    public String handleDeclare(int receivedLeaderId) {
        String electionType = config.electionType();
        
        // Set the leader state
        setLeaderState(receivedLeaderId);
        
        // Reset voting for next term (Raft)
        votedFor.set(-1);
        votesReceived.set(0);
        
        if (electionType.equals("ring")) {
            // In ring, forward declare unless it's our own declaration coming back
            if (receivedLeaderId != config.electionId()) {
                sendToPeerAsync(0, "declare " + receivedLeaderId);
            }
        }
        
        return "ack " + config.electionId();
    }

    /**
     * Handle incoming vote command (Raft only)
     */
    public void handleVote(int senderId, int candidateId) {
        if (candidateId == config.electionId()) {
            int votes = votesReceived.incrementAndGet();
            int majority = (config.electionPeerIds().length + 1) / 2 + 1;
            
            if (votes >= majority && state.get() == ElectionState.CANDIDATE) {
                declareAsLeader();
            }
        }
    }

    /**
     * Handle heartbeat (ping)
     */
    public String handlePing() {
        lastHeartbeatTime = System.currentTimeMillis();
        return "pong";
    }

    /**
     * Declare this broker as leader and notify all peers
     */
    private void declareAsLeader() {
        // Set leader state first (without heartbeats)
        setLeaderState(config.electionId());
        
        // Send declare to all peers synchronously
        for (int i = 0; i < config.electionPeerIds().length; i++) {
            sendToPeerSync(i, "declare " + config.electionId());
        }
        
        // Now start heartbeats after declares are sent
        startHeartbeatSender();
    }

    /**
     * Register with DNS as the leader
     */
    private void registerAsDnsLeader() {
        if (dnsClient != null && config.electionDomain() != null) {
            String address = config.host() + ":" + config.port();
            dnsClient.registerDomain(config.electionDomain(), address);
        }
    }

    /**
     * Start sending heartbeats to followers (leader only)
     */
    private void startHeartbeatSender() {
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        
        heartbeatThread = Thread.ofVirtual().start(() -> {
            // Calculate heartbeat interval: should be less than half the minimum timeout
            long interval = Math.max(10, config.electionHeartbeatTimeoutMs() / 3);
            
            while (running && state.get() == ElectionState.LEADER) {
                for (int i = 0; i < config.electionPeerIds().length; i++) {
                    sendToPeerAsync(i, "ping");
                }
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    /**
     * Start monitoring for heartbeat timeout (follower/candidate)
     */
    public void startHeartbeatTimeoutMonitor() {
        if (config.electionType() == null || config.electionType().equals("none")) {
            return;
        }
        
        heartbeatTimeoutThread = Thread.ofVirtual().start(() -> {
            while (running) {
                try {
                    Thread.sleep(config.electionHeartbeatTimeoutMs() / 2);
                    
                    if (state.get() != ElectionState.LEADER) {
                        long elapsed = System.currentTimeMillis() - lastHeartbeatTime;
                        if (elapsed > config.electionHeartbeatTimeoutMs()) {
                            // Timeout reached, initiate election
                            initiateElection();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    /**
     * Send a message to a peer asynchronously (fire and forget)
     */
    private void sendToPeerAsync(int peerIndex, String message) {
        if (peerIndex < 0 || peerIndex >= config.electionPeerHosts().length) {
            return;
        }
        
        String host = config.electionPeerHosts()[peerIndex];
        int port = config.electionPeerPorts()[peerIndex];
        
        Thread.ofVirtual().start(() -> {
            try (Socket socket = new Socket(host, port);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                
                // Read greeting
                in.readLine();
                
                // Send message
                out.println(message);
                
                // Read response (and ignore for now)
                in.readLine();
            } catch (IOException e) {
                // Peer not available, try next peer in ring if applicable
                if (config.electionType().equals("ring") && (message.startsWith("elect") || message.startsWith("declare"))) {
                    // Try next peer
                    int nextPeer = (peerIndex + 1) % config.electionPeerHosts().length;
                    if (nextPeer != peerIndex) {
                        sendToPeerAsync(nextPeer, message);
                    }
                }
            }
        });
    }
    
    /**
     * Send a message to a peer synchronously and wait for completion
     */
    private void sendToPeerSync(int peerIndex, String message) {
        if (peerIndex < 0 || peerIndex >= config.electionPeerHosts().length) {
            return;
        }
        
        String host = config.electionPeerHosts()[peerIndex];
        int port = config.electionPeerPorts()[peerIndex];
        
        try (Socket socket = new Socket(host, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            socket.setSoTimeout(500);
            
            // Read greeting
            in.readLine();
            
            // Send message
            out.println(message);
            
            // Read response
            in.readLine();
        } catch (IOException e) {
            // Peer not available, try next peer in ring if applicable
            if (config.electionType().equals("ring") && (message.startsWith("elect") || message.startsWith("declare"))) {
                // Try next peer
                int nextPeer = (peerIndex + 1) % config.electionPeerHosts().length;
                if (nextPeer != peerIndex) {
                    sendToPeerSync(nextPeer, message);
                }
            }
        }
    }

    /**
     * Send a message to a peer and wait for response
     */
    private String sendToPeerAndGetResponse(int peerIndex, String message) {
        if (peerIndex < 0 || peerIndex >= config.electionPeerHosts().length) {
            return null;
        }
        
        String host = config.electionPeerHosts()[peerIndex];
        int port = config.electionPeerPorts()[peerIndex];
        
        try (Socket socket = new Socket(host, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            socket.setSoTimeout(500); // 500ms timeout
            
            // Read greeting
            in.readLine();
            
            // Send message
            out.println(message);
            
            // Read response
            return in.readLine();
        } catch (IOException e) {
            return null;
        }
    }

    public void shutdown() {
        running = false;
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        if (heartbeatTimeoutThread != null) {
            heartbeatTimeoutThread.interrupt();
        }
        
        // Close all peer connections
        for (Socket socket : peerConnections.values()) {
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    public void resetHeartbeatTimer() {
        lastHeartbeatTime = System.currentTimeMillis();
    }

    public void resetVoting() {
        votedFor.set(-1);
        votesReceived.set(0);
    }
}
