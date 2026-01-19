package com.msgbroker.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Handles LEP (Leader Election Protocol) for a single peer connection
 */
public class ElectionConnectionHandler implements Runnable {

    private final Socket socket;
    private final ElectionManager electionManager;

    public ElectionConnectionHandler(Socket socket, ElectionManager electionManager) {
        this.socket = socket;
        this.electionManager = electionManager;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            // Send LEP greeting
            out.println("ok LEP");

            String msg;
            while ((msg = in.readLine()) != null) {
                String[] parts = msg.trim().split("\\s+");

                if (parts.length == 0 || parts[0].isEmpty()) {
                    continue;
                }

                switch (parts[0]) {
                    case "elect" -> {
                        if (parts.length != 2) {
                            out.println("error usage: elect <id>");
                            continue;
                        }
                        try {
                            int id = Integer.parseInt(parts[1]);
                            String response = electionManager.handleElect(id, out);
                            out.println(response);
                        } catch (NumberFormatException e) {
                            out.println("error usage: elect <id>");
                        }
                    }
                    case "declare" -> {
                        if (parts.length != 2) {
                            out.println("error usage: declare <id>");
                            continue;
                        }
                        try {
                            int id = Integer.parseInt(parts[1]);
                            String response = electionManager.handleDeclare(id);
                            out.println(response);
                        } catch (NumberFormatException e) {
                            out.println("error usage: declare <id>");
                        }
                    }
                    case "vote" -> {
                        if (parts.length != 3) {
                            out.println("error usage: vote <sender-id> <candidate-id>");
                            continue;
                        }
                        try {
                            int senderId = Integer.parseInt(parts[1]);
                            int candidateId = Integer.parseInt(parts[2]);
                            electionManager.handleVote(senderId, candidateId);
                            // No response for vote
                        } catch (NumberFormatException e) {
                            out.println("error usage: vote <sender-id> <candidate-id>");
                        }
                    }
                    case "ping" -> {
                        String response = electionManager.handlePing();
                        out.println(response);
                    }
                    case "exit" -> {
                        out.println("ok bye");
                        return;
                    }
                    default -> out.println("error protocol error");
                }
            }
        } catch (IOException e) {
            // Connection closed
        } finally {
            closeSocket();
        }
    }

    private void closeSocket() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
    }
}
