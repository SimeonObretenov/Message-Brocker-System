package com.msgbroker.cli;

import com.msgbroker.client.IClient;
import com.msgbroker.config.Config;
import com.msgbroker.connection.Channel;
import com.msgbroker.connection.types.ExchangeType;
import com.msgbroker.connection.Subscription;

import java.io.*;
import java.util.Locale;

public class ClientCLI implements IClientCLI {

    private final IClient client;
    private final Config config;
    private final BufferedReader reader;
    private final PrintStream writer;
    private Channel channel;

    public ClientCLI(IClient client, Config config, InputStream in, OutputStream out) {
        this.client = client;
        this.config = config;
        this.reader = new BufferedReader(new InputStreamReader(in));
        this.writer = new PrintStream(out, true);
    }

    @Override
    public void run() {
        try {
            String line;
            while (true) {
                printPrompt();
                line = reader.readLine();
                if (line == null) break;

                String[] args = line.trim().split("\\s+");
                if (args.length == 0 || args[0].isEmpty()) continue;

                switch (args[0]) {
                    case "channel" -> handleChannel(args);
                    case "subscribe" -> handleSubscribe(args);
                    case "publish" -> handlePublish(args);
                    case "shutdown" -> { handleShutdown(); return; }
                    default -> writer.println("error");
                }
            }
        } catch (IOException e) {
            writer.println("error");
        }
    }

    @Override
    public void printPrompt() {
        writer.print(client.getComponentId() + "> ");
        writer.flush();
    }

    /**
     * A channel is an instance which is used to multiplex connections on a single TCP connection.
     * Please read the documentation of the {@link Channel} class for more information.
     * Attention should be paid if the channel is already connected to a broker, it should be disconnected first.
     *
     * @param broker the broker to which the channel should be created
     * @return the channel to which a connection is established with the specified broker
     */
    private Channel createChannel(String broker) {
        try {
            String host = config.getString(broker + ".host");
            int port = config.getInt(broker + ".port");

            if (channel != null) channel.disconnect();

            Channel newChannel = new Channel(host, port);
            if (newChannel.connect()) {
                return newChannel;
            }
        } catch (Exception ignored) { }
        return null;
    }

    private void handleChannel(String[] args) {
        if (args.length != 2) {
            writer.println("error");
            return;
        }
        channel = createChannel(args[1]);
        writer.println(channel != null ? "ok" : "error");
    }

    private void handleSubscribe(String[] args) {
        if (args.length != 5) {
            writer.println("error");
            return;
        }
        if (channel == null) {
            writer.println("error");
            return;
        }

        try {
            ExchangeType type = ExchangeType.valueOf(args[2].toUpperCase(Locale.ROOT));

            if (!channel.exchangeDeclare(type, args[1])) {
                writer.println("error");
                return;
            }
            if (!channel.queueBind(args[3], args[4])) {
                writer.println("error");
                return;
            }

            Thread sub = channel.subscribe(msg -> writer.println(msg));
            if (sub == null) {
                writer.println("error");
                return;
            }

            writer.println("ok");

            reader.readLine();

            sub.interrupt();
            sub.join(200);

        } catch (Exception e) {
            writer.println("error");
        }
    }


    private void handlePublish(String[] args) {
        if (args.length != 5) {
            writer.println("error");
            return;
        }
        if (channel == null) {
            writer.println("error");
            return;
        }

        try {
            ExchangeType type = ExchangeType.valueOf(args[2].toUpperCase(Locale.ROOT));

            if (!channel.exchangeDeclare(type, args[1]) || !channel.publish(args[3], args[4])) {
                writer.println("error");
                return;
            }

            writer.println("ok");
        } catch (Exception e) {
            writer.println("error");
        }
    }

    private void handleShutdown() {
        try {
            if (channel != null) channel.disconnect();
        } catch (Exception ignored) {}
        client.shutdown();
    }
}
