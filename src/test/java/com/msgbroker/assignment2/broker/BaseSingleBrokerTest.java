package com.msgbroker.assignment2.broker;

import com.msgbroker.ComponentFactory;
import com.msgbroker.broker.IBroker;
import com.msgbroker.config.BrokerConfig;
import com.msgbroker.util.ConfigFactory;
import com.msgbroker.util.Constants;
import com.msgbroker.util.Util;
import com.msgbroker.util.helper.TelnetClientHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public abstract class BaseSingleBrokerTest {

    protected static final BrokerConfig config = ConfigFactory.createBrokerConfigA2();

    protected IBroker broker;
    protected Thread brokerThread;

    protected abstract void initTelnetClientHelpers() throws IOException;

    protected abstract void closeTelnetClientHelpers() throws IOException;

    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    @BeforeEach
    void beforeEach() throws IOException {
        broker = ComponentFactory.createBroker(config);
        brokerThread = new Thread(broker);
        brokerThread.start();

        TelnetClientHelper waitForConnHelper = new TelnetClientHelper(Constants.LOCALHOST, config.port());
        waitForConnHelper.waitForInitConnection();
        waitForConnHelper.disconnect();

        initTelnetClientHelpers();
    }

    @AfterEach
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void afterEach() {
        try {
            closeTelnetClientHelpers();
        } catch (IOException e) {
            // ignored
        }

        if (broker != null) {
            broker.shutdown();
        }

        try {
            if (brokerThread != null && brokerThread.isAlive()) {
                brokerThread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Util.waitForTcpPortsToClose(config.port());
    }
}
