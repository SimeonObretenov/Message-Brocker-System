package com.msgbroker.connection;

import com.msgbroker.connection.types.ExchangeType;

import java.io.*;
import java.net.Socket;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Consumer;

public class Channel implements IChannel {

    private final String host;
    private final int port;

    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public Channel(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public boolean connect() throws IOException {
        try {
            socket = new Socket(host, port);
            out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            String response = in.readLine();
            return response != null && response.trim().equalsIgnoreCase("ok SMQP");
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void disconnect() {
        try {
            if (out != null) {
                out.println("exit");
                out.flush();
            }
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException ignored) {
        }
    }

    @Override
    public boolean exchangeDeclare(ExchangeType exchangeType, String exchangeName) {
        return sendAndExpectOk("exchange " + exchangeType.name().toLowerCase() + " " + exchangeName);
    }

    @Override
    public boolean queueBind(String queueName, String bindingKey) {
        if (!sendAndExpectOk("queue " + queueName)) return false;
        return sendAndExpectOk("bind " + bindingKey);
    }

    @Override
    public Thread subscribe(Consumer<String> callback) {
        try {
            out.println("subscribe");
            out.flush();
            String response = in.readLine();
            if (response == null || !response.trim().equalsIgnoreCase("ok")) return null;
        } catch (IOException e) {
            return null;
        }

        Thread t = new Thread(() -> {
            try {
                String msg;
                while ((msg = in.readLine()) != null && !Thread.currentThread().isInterrupted()) {
                    callback.accept(msg);
                }
            } catch (IOException ignored) {}
        });
        t.setDaemon(true);
        t.start();
        return t;
    }

    @Override
    public String getFromSubscription() {
        try {
            return in.readLine();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public boolean publish(String routingKey, String message) {
        return sendAndExpectOk("publish " + routingKey + " " + message);
    }

    private boolean sendAndExpectOk(String command) {
        try {
            out.println(command);
            out.flush();
            String response = in.readLine();
            return response != null && response.trim().equalsIgnoreCase("ok");
        } catch (IOException e) {
            return false;
        }
    }
}
