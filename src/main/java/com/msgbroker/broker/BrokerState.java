package com.msgbroker.broker;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared state for the broker that is accessed by multiple client handlers
 */
public class BrokerState {

    // Store exchanges by name
    private final ConcurrentHashMap<String, Exchange> exchanges;

    // Store queues by name
    private final ConcurrentHashMap<String, MessageQueue> queues;

    public BrokerState() {
        this.exchanges = new ConcurrentHashMap<>();
        this.queues = new ConcurrentHashMap<>();
        
        // Create the default exchange at startup with type "default"
        exchanges.put("default", new Exchange("default", "default"));
    }

    public Exchange getExchange(String name) {
        return exchanges.get(name);
    }

    public Exchange createExchange(String name, String type) {
        return exchanges.computeIfAbsent(name, k -> new Exchange(name, type));
    }

    public Exchange getOrCreateExchange(String name, String type) {
        Exchange existing = exchanges.get(name);
        if (existing != null) {
            if (!existing.getType().equals(type)) {
                return null; // Type mismatch
            }
            return existing;
        }
        return exchanges.computeIfAbsent(name, k -> new Exchange(name, type));
    }

    public MessageQueue getQueue(String name) {
        return queues.get(name);
    }

    public MessageQueue createQueue(String name) {
        return queues.computeIfAbsent(name, k -> {
            MessageQueue queue = new MessageQueue(name);
            // Auto-bind to default exchange
            Exchange defaultExchange = exchanges.get("default");
            if (defaultExchange != null) {
                defaultExchange.bind(name, queue);
            }
            return queue;
        });
    }

    public MessageQueue getOrCreateQueue(String name) {
        return queues.computeIfAbsent(name, k -> {
            MessageQueue queue = new MessageQueue(name);
            // Auto-bind to default exchange
            Exchange defaultExchange = exchanges.get("default");
            if (defaultExchange != null) {
                defaultExchange.bind(name, queue);
            }
            return queue;
        });
    }
}
