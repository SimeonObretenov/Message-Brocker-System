package com.msgbroker.broker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents an exchange that routes messages to queues
 */
public class Exchange {

    private final String name;
    private final String type; // direct, fanout, topic, default
    private final Map<String, List<MessageQueue>> bindings;

    public Exchange(String name, String type) {
        this.name = name;
        this.type = type;
        this.bindings = new ConcurrentHashMap<>();
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    /**
     * Bind a queue to this exchange with a binding key
     */
    public void bind(String bindingKey, MessageQueue queue) {
        bindings.computeIfAbsent(bindingKey, k -> new ArrayList<>()).add(queue);
    }

    /**
     * Route a message to appropriate queues based on routing key
     */
    public void routeMessage(String routingKey, String message) {
        switch (type) {
            case "direct", "default" -> routeDirect(routingKey, message);
            case "fanout" -> routeFanout(message);
            case "topic" -> routeTopic(routingKey, message);
        }
    }

    private void routeDirect(String routingKey, String message) {
        List<MessageQueue> queues = bindings.get(routingKey);
        if (queues != null) {
            for (MessageQueue queue : queues) {
                queue.enqueue(message);
            }
        }
    }

    private void routeFanout(String message) {
        for (List<MessageQueue> queueList : bindings.values()) {
            for (MessageQueue queue : queueList) {
                queue.enqueue(message);
            }
        }
    }

    private void routeTopic(String routingKey, String message) {
        String[] routingParts = routingKey.split("\\.");
        
        for (Map.Entry<String, List<MessageQueue>> entry : bindings.entrySet()) {
            String bindingKey = entry.getKey();
            if (matchesTopicPattern(routingParts, bindingKey)) {
                for (MessageQueue queue : entry.getValue()) {
                    queue.enqueue(message);
                }
            }
        }
    }

    /**
     * Match routing key against topic pattern
     * * matches exactly one word
     * # matches zero or more words
     */
    private boolean matchesTopicPattern(String[] routingParts, String bindingKey) {
        String[] bindingParts = bindingKey.split("\\.");
        
        int r = 0, b = 0;
        while (r < routingParts.length && b < bindingParts.length) {
            if (bindingParts[b].equals("#")) {
                // # can match zero or more words
                if (b == bindingParts.length - 1) {
                    return true; // # at the end matches everything remaining
                }
                // Try to match the rest
                for (int i = r; i <= routingParts.length; i++) {
                    if (matchesTopicPatternFrom(routingParts, i, bindingParts, b + 1)) {
                        return true;
                    }
                }
                return false;
            } else if (bindingParts[b].equals("*") || bindingParts[b].equals(routingParts[r])) {
                r++;
                b++;
            } else {
                return false;
            }
        }
        
        // Handle trailing #
        while (b < bindingParts.length && bindingParts[b].equals("#")) {
            b++;
        }
        
        return r == routingParts.length && b == bindingParts.length;
    }

    private boolean matchesTopicPatternFrom(String[] routingParts, int r, String[] bindingParts, int b) {
        while (r < routingParts.length && b < bindingParts.length) {
            if (bindingParts[b].equals("#")) {
                if (b == bindingParts.length - 1) {
                    return true;
                }
                for (int i = r; i <= routingParts.length; i++) {
                    if (matchesTopicPatternFrom(routingParts, i, bindingParts, b + 1)) {
                        return true;
                    }
                }
                return false;
            } else if (bindingParts[b].equals("*") || bindingParts[b].equals(routingParts[r])) {
                r++;
                b++;
            } else {
                return false;
            }
        }
        
        while (b < bindingParts.length && bindingParts[b].equals("#")) {
            b++;
        }
        
        return r == routingParts.length && b == bindingParts.length;
    }
}
