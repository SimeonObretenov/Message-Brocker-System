package com.msgbroker;

import com.msgbroker.broker.Broker;
import com.msgbroker.broker.IBroker;
import com.msgbroker.config.BrokerConfig;
import com.msgbroker.config.ConfigParser;
import com.msgbroker.config.DNSServerConfig;
import com.msgbroker.config.MonitoringServerConfig;
import com.msgbroker.dns.DNSServer;
import com.msgbroker.dns.IDNSServer;
import com.msgbroker.monitoring.IMonitoringServer;
import com.msgbroker.monitoring.MonitoringServer;

public class ComponentFactory {
    /**
     * Creates a broker via the given config. Used for Testing
     * @param config config of the broker
     * @return a new broker
     */
    public static IBroker createBroker(BrokerConfig config) {
        return new Broker(config);
    }

    /**
     * Creates a broker via the .properties file
     * @param componentId name of server (e.g "broker-0")
     * @return a new broker
     */
    public static IBroker createBroker(String componentId) {
        ConfigParser parser = new ConfigParser(componentId);

        BrokerConfig brokerConfig = parser.toBrokerConfig();

        return createBroker(brokerConfig);
    }

    public static IDNSServer createDNSServer(DNSServerConfig config) {
        return new DNSServer(config);
    }

    public static IDNSServer createDNSServer(String componentId) {
        ConfigParser parser = new ConfigParser(componentId);
        DNSServerConfig dnsServerConfig = parser.toDNSServerConfig();

        return createDNSServer(dnsServerConfig);
    }

    public static IMonitoringServer createMonitoringServer(MonitoringServerConfig config) {
        return new MonitoringServer(config);
    }

    public static IMonitoringServer createMonitoringServer(String componentId) {
        ConfigParser parser = new ConfigParser(componentId);
        MonitoringServerConfig monitoringServerConfig = parser.toMonitoringServerConfig();

        return createMonitoringServer(monitoringServerConfig);
    }
}
