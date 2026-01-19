# Message Broker System

A distributed message broker system built in Java featuring publish/subscribe messaging, DNS service discovery, leader election algorithms, and real-time monitoring.

## Features

- **Message Broker**: Publish/subscribe messaging with topic-based routing and exchange types (direct, fanout, topic)
- **DNS Service Discovery**: Dynamic service registration and lookup for broker instances
- **Leader Election**: Implementation of multiple election algorithms:
  - Bully Algorithm
  - Ring Algorithm  
  - Raft Consensus
- **Monitoring Server**: Real-time system monitoring via UDP with statistics collection
- **Multi-Broker Support**: Run multiple broker instances with automatic failover
- **Client CLI**: Interactive command-line interface for publishing and subscribing

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Client    │────▶│   Broker    │◀────│   Client    │
└─────────────┘     └──────┬──────┘     └─────────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
       ┌──────────┐  ┌──────────┐  ┌──────────┐
       │   DNS    │  │ Monitor  │  │  Broker  │
       │  Server  │  │  Server  │  │ (Replica)│
       └──────────┘  └──────────┘  └──────────┘
```

## Requirements

- Java 21 or higher
- Maven 3.6+

## Quick Start

1. **Build the project**
   ```bash
   mvn clean compile
   ```

2. **Start the DNS Server** (required for service discovery)
   ```bash
   mvn exec:java@dns-0
   ```

3. **Start a Broker** (in a new terminal)
   ```bash
   mvn exec:java@broker-0
   ```

4. **Start a Client** (in a new terminal)
   ```bash
   mvn exec:java@client-0
   ```

## Running Multiple Instances

### Brokers
```bash
mvn exec:java@broker-0
mvn exec:java@broker-1
mvn exec:java@broker-2
```

### Clients
```bash
mvn exec:java@client-0
mvn exec:java@client-1
mvn exec:java@client-2
```

### Monitoring Server
```bash
mvn exec:java@monitoring-0
```

## Testing

Run all tests:
```bash
mvn test
```

The test suite includes:
- Broker protocol and exchange tests
- DNS registration and lookup tests
- Leader election algorithm tests (Bully, Ring, Raft)
- Monitoring server integration tests
- Client CLI tests

## Configuration

Configuration files are located in `src/main/resources/`:

| File | Description |
|------|-------------|
| `broker-*.properties` | Broker instance configurations (port, election type, peers) |
| `client-*.properties` | Client configurations (broker host/port) |
| `dns-0.properties` | DNS server configuration |
| `monitoring-0.properties` | Monitoring server UDP port configuration |

## Project Structure

```
src/
├── main/java/com/msgbroker/
│   ├── broker/         # Message broker, exchanges, queues, election
│   ├── client/         # Client implementation
│   ├── cli/            # Command-line interface
│   ├── config/         # Configuration parsing
│   ├── connection/     # Channel and subscription handling
│   ├── dns/            # DNS server for service discovery
│   └── monitoring/     # UDP monitoring server
└── test/               # Comprehensive test suite
```

## Technologies

- **Java 21** - Virtual threads for concurrent connections
- **Maven** - Build and dependency management
- **JUnit 5** - Testing framework
- **SLF4J** - Logging facade
- **Lombok** - Boilerplate reduction

## License

MIT License

MIT License
