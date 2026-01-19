package com.msgbroker.dns;

import com.msgbroker.IServer;

/**
 * Interface for a DNS server.
 * <p>
 * NOTE: Do not delete this interface as it is part of the test suite and required for the next assignment.
 */
public interface IDNSServer extends IServer {


    /**
     * Implement the main logic of the DNS server.
     * This method should start the DNS server and listen for incoming DNS requests.
     * It is recommended to create separate classes for the broker logic to keep the code clean and structured.
     */
    @Override
    void run();

    /**
     * Implement a graceful shutdown of the DNS server.
     * That means all system resources should be released and running threads should be stopped gracefully.
     */
    @Override
    void shutdown();
}
