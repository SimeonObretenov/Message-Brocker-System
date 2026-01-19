package com.msgbroker;

/**
 * Interface for a server.
 * <p>
 * NOTE: Do not delete this interface as it is required for this assignment.
 */
public interface IServer extends Runnable {

    void shutdown();
}
