package com.msgbroker.dns;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class DNSConnectionHandler implements Runnable {

    private final Socket socket;
    private final ConcurrentHashMap<String, String> domainRegistry;

    public DNSConnectionHandler(Socket socket, ConcurrentHashMap<String, String> domainRegistry) {
        this.socket = socket;
        this.domainRegistry = domainRegistry;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            // Send SDP greeting
            out.println("ok SDP");

            String msg;
            loop:
            while ((msg = in.readLine()) != null) {
                String[] parts = msg.trim().split("\\s+");

                if (parts.length == 0 || parts[0].isEmpty()) {
                    continue;
                }

                switch (parts[0]) {
                    case "register" -> {
                        if (parts.length != 3) {
                            out.println("error usage: register <name> <ip:port>");
                            break;
                        }
                        handleRegister(parts[1], parts[2], out);
                    }
                    case "unregister" -> {
                        if (parts.length != 2) {
                            out.println("error usage: unregister <name>");
                            break;
                        }
                        handleUnregister(parts[1], out);
                    }
                    case "resolve" -> {
                        if (parts.length != 2) {
                            out.println("error usage: resolve <name>");
                            break;
                        }
                        handleResolve(parts[1], out);
                    }
                    case "exit" -> {
                        out.println("ok bye");
                        break loop;
                    }
                    default -> out.println("error usage: <command> <args>");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeSocket();
        }
    }

    private void handleRegister(String name, String address, PrintWriter out) {
        // Validate IP:port format
        if (!address.matches(".+:\\d+")) {
            out.println("error invalid address format");
            return;
        }

        try {
            domainRegistry.put(name, address);
            out.println("ok");
        } catch (Exception e) {
            out.println("error " + e.getMessage());
        }
    }

    private void handleUnregister(String name, PrintWriter out) {
        // Always returns ok, even if domain doesn't exist
        domainRegistry.remove(name);
        out.println("ok");
    }

    private void handleResolve(String name, PrintWriter out) {
        String address = domainRegistry.get(name);
        if (address != null) {
            out.println(address);
        } else {
            out.println("error domain not found");
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
