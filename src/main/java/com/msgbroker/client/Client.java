package com.msgbroker.client;

import com.msgbroker.ComponentFactory;
import com.msgbroker.cli.ClientCLI;
import com.msgbroker.config.Config;
import com.msgbroker.connection.Channel;
import com.msgbroker.connection.Subscription;

import java.io.InputStream;
import java.io.OutputStream;

public class Client implements IClient {

    private final String componentId;
    private final InputStream in;
    private final OutputStream out;

    private ClientCLI cli;
    private Config config;

    public Client(String componentId, InputStream in, OutputStream out) {
        this.componentId = componentId;
        this.in = in;
        this.out = out;
    }

    @Override
    public void run() {
        try {
            config = new Config(componentId);

            cli = new ClientCLI(this, config, in, out);
            cli.run();

        } catch (Exception e) {
            System.err.println("error: " + e.getMessage());
        }
    }

    @Override
    public String getComponentId() {
        return componentId;
    }

    @Override
    public void shutdown() {
        try {
            if (cli != null) {
            }
        } catch (Exception ignored) {}
    }

    /**
     * Creates a new {@link Client} and runs it.
     * Standard input, output and error streams are passed to the client which are then used by the {@link ClientCLI}.
     *
     * @param args the client config filename found in classpath resources without the file extension
     */
    public static void main(String[] args) {
        new Client(args[0], System.in, System.out).run();
    }
}
