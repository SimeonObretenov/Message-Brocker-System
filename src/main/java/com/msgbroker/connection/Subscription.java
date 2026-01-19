package com.msgbroker.connection;

import java.util.function.Consumer;

public class Subscription extends Thread {

    private final IChannel channel;
    private final Consumer<String> callback;
    private volatile boolean running = true;

    public Subscription(IChannel channel, Consumer<String> callback) {
        this.channel = channel;
        this.callback = callback;
        setDaemon(true);
    }

    @Override
    public void run() {
        while (running) {
            try {
                String message = channel.getFromSubscription();
                if (message == null) break;
                callback.accept(message);
            } catch (Exception e) {
                break;
            }
        }
    }
}

