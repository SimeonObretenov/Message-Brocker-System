# Message Broker System

A distributed message broker system built in Java featuring service discovery, monitoring capabilities, and multiple client support.

## Features

- **Message Broker**: Handles message routing and delivery between publishers and subscribers
- **DNS Service Discovery**: Dynamic service registration and lookup
- **Monitoring Server**: Real-time system monitoring and health checks
- **Client Management**: Support for multiple concurrent clients

## Requirements

- Java 21 or higher
- Maven 3.6+

## Project Structure

```
src/
├── main/
│   ├── java/com/msgbroker/
│   │   ├── broker/       # Message broker implementation
│   │   ├── client/       # Client components
│   │   ├── cli/          # Command line interface
│   │   ├── config/       # Configuration management
│   │   ├── connection/   # Connection handling
│   │   ├── dns/          # DNS service discovery
│   │   └── monitoring/   # Monitoring server
│   └── resources/        # Configuration files
└── test/                 # Unit tests
```

## Building

```bash
mvn clean compile
```

## Running

### Start a Broker Instance
```bash
mvn exec:java@broker-0
mvn exec:java@broker-1
mvn exec:java@broker-2
```

### Start DNS Server
```bash
mvn exec:java@dns-0
```

### Start Monitoring Server
```bash
mvn exec:java@monitoring-0
```

### Start Client Instances
```bash
mvn exec:java@client-0
mvn exec:java@client-1
mvn exec:java@client-2
```

## Testing

```bash
mvn test
```

## Configuration

Configuration files are located in `src/main/resources/`:
- `broker-*.properties` - Broker instance configurations
- `client-*.properties` - Client configurations
- `dns-0.properties` - DNS server configuration
- `monitoring-0.properties` - Monitoring server configuration

## License

MIT License
