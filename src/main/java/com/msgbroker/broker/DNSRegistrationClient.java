package com.msgbroker.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Helper class to register the broker with the DNS server
 */
public class DNSRegistrationClient {

    private final String dnsHost;
    private final int dnsPort;

    public DNSRegistrationClient(String dnsHost, int dnsPort) {
        this.dnsHost = dnsHost;
        this.dnsPort = dnsPort;
    }

    /**
     * Registers a domain with the DNS server
     * @param domain The domain name to register
     * @param address The IP:port address of the broker
     * @return true if registration was successful, false otherwise
     */
    public boolean registerDomain(String domain, String address) {
        try (Socket socket = new Socket(dnsHost, dnsPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            // Read greeting
            String greeting = in.readLine();
            if (!"ok SDP".equals(greeting)) {
                System.err.println("Unexpected DNS greeting: " + greeting);
                return false;
            }

            // Send register command
            out.println("register " + domain + " " + address);
            String response = in.readLine();

            if ("ok".equals(response)) {
                System.out.println("Successfully registered " + domain + " at DNS");
                return true;
            } else {
                System.err.println("DNS registration failed: " + response);
                return false;
            }

        } catch (IOException e) {
            System.err.println("Failed to connect to DNS server: " + e.getMessage());
            return false;
        }
    }

    /**
     * Unregisters a domain from the DNS server
     * @param domain The domain name to unregister
     * @return true if unregistration was successful, false otherwise
     */
    public boolean unregisterDomain(String domain) {
        try (Socket socket = new Socket(dnsHost, dnsPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            // Read greeting
            in.readLine();

            // Send unregister command
            out.println("unregister " + domain);
            String response = in.readLine();

            if ("ok".equals(response)) {
                System.out.println("Successfully unregistered " + domain + " from DNS");
                return true;
            } else {
                System.err.println("DNS unregistration failed: " + response);
                return false;
            }

        } catch (IOException e) {
            System.err.println("Failed to connect to DNS server: " + e.getMessage());
            return false;
        }
    }
}
