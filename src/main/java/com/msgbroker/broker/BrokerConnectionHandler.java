package com.msgbroker.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.function.Consumer;

/**
 * Handles SMQP protocol for a single client connection
 */
public class BrokerConnectionHandler implements Runnable {

    private final Socket socket;
    private final BrokerState brokerState;
    private final Consumer<String> monitoringCallback;
    
    // Session state for this client
    private Exchange currentExchange;
    private MessageQueue currentQueue;
    private volatile boolean stopSubscription = false;

    public BrokerConnectionHandler(Socket socket, BrokerState brokerState, Consumer<String> monitoringCallback) {
        this.socket = socket;
        this.brokerState = brokerState;
        this.monitoringCallback = monitoringCallback;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            // Send SMQP greeting
            out.println("ok SMQP");

            String msg;
            loop:
            while ((msg = in.readLine()) != null) {
                String[] parts = msg.trim().split("\\s+", 3);

                if (parts.length == 0 || parts[0].isEmpty()) {
                    continue;
                }

                switch (parts[0]) {
                    case "exchange" -> {
                        if (parts.length != 3) {
                            out.println("error usage: exchange <type> <name>");
                            break;
                        }
                        handleExchange(parts[1], parts[2], out);
                    }
                    case "queue" -> {
                        if (parts.length != 2) {
                            out.println("error usage: queue <name>");
                            break;
                        }
                        handleQueue(parts[1], out);
                    }
                    case "bind" -> {
                        if (parts.length != 2) {
                            out.println("error usage: bind <binding-key>");
                            break;
                        }
                        handleBind(parts[1], out);
                    }
                    case "publish" -> {
                        if (parts.length < 3) {
                            out.println("error usage: publish <routing-key> <message>");
                            break;
                        }
                        handlePublish(parts[1], parts[2], out);
                    }
                    case "subscribe" -> {
                        handleSubscribe(in, out);
                        // After subscribe ends, continue processing commands
                    }
                    case "stop" -> {
                        stopSubscription = true;
                    }
                    case "exit" -> {
                        out.println("ok bye");
                        break loop;
                    }
                    default -> out.println("error unknown command");
                }
            }
        } catch (IOException e) {
            // Connection closed
        } finally {
            closeSocket();
        }
    }

    private void handleExchange(String type, String name, PrintWriter out) {
        // Validate exchange type
        if (!type.equals("direct") && !type.equals("fanout") && !type.equals("topic") && !type.equals("default")) {
            out.println("error invalid exchange type");
            return;
        }

        Exchange exchange = brokerState.getOrCreateExchange(name, type);
        if (exchange == null) {
            out.println("error exchange already exists with different type");
            return;
        }

        currentExchange = exchange;
        out.println("ok");
    }

    private void handleQueue(String name, PrintWriter out) {
        currentQueue = brokerState.getOrCreateQueue(name);
        out.println("ok");
    }

    private void handleBind(String bindingKey, PrintWriter out) {
        if (currentExchange == null) {
            out.println("error no exchange declared");
            return;
        }
        if (currentQueue == null) {
            out.println("error no queue declared");
            return;
        }

        currentExchange.bind(bindingKey, currentQueue);
        out.println("ok");
    }

    private void handlePublish(String routingKey, String message, PrintWriter out) {
        if (currentExchange == null) {
            out.println("error no exchange declared");
            return;
        }

        currentExchange.routeMessage(routingKey, message);
        
        // Send monitoring data
        if (monitoringCallback != null) {
            monitoringCallback.accept(routingKey);
        }
        
        out.println("ok");
    }

    private void handleSubscribe(BufferedReader in, PrintWriter out) {
        if (currentQueue == null) {
            out.println("error no queue declared");
            return;
        }

        // Send acknowledgment that subscription started
        out.println("ok");
        out.flush();

        stopSubscription = false;

        // Start a message delivery thread
        Thread deliveryThread = Thread.ofVirtual().start(() -> {
            try {
                while (!stopSubscription && !socket.isClosed()) {
                    String message = currentQueue.poll(50, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (message != null && !stopSubscription) {
                        synchronized (out) {
                            out.println(message);
                            out.flush();
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // Main thread listens for stop command
        try {
            String cmd;
            while ((cmd = in.readLine()) != null) {
                if (cmd.trim().equals("stop")) {
                    stopSubscription = true;
                    break;
                }
                // Ignore other commands during subscription
            }
        } catch (IOException e) {
            stopSubscription = true;
        } finally {
            try {
                deliveryThread.join(1000); // Wait for delivery thread to finish
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void closeSocket() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
