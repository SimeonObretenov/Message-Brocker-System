package com.msgbroker.broker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Represents a message queue that stores messages for subscribers
 */
public class MessageQueue {

    private final String name;
    private final BlockingQueue<String> messages;

    public MessageQueue(String name) {
        this.name = name;
        this.messages = new LinkedBlockingQueue<>();
    }

    public String getName() {
        return name;
    }

    /**
     * Add a message to the queue
     */
    public void enqueue(String message) {
        messages.offer(message);
    }

    /**
     * Take a message from the queue (blocks if empty)
     */
    public String dequeue() throws InterruptedException {
        return messages.take();
    }

    /**
     * Poll a message from the queue with timeout
     */
    public String poll(long timeout, java.util.concurrent.TimeUnit unit) throws InterruptedException {
        return messages.poll(timeout, unit);
    }

    /**
     * Check if queue is empty
     */
    public boolean isEmpty() {
        return messages.isEmpty();
    }
}
