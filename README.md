# ShardWizard

[![Maven Central](https://img.shields.io/maven-central/v/com.dream11/shard-wizard)](https://search.maven.org/artifact/com.dream11/shard-wizard)
[![Java Version](https://img.shields.io/badge/java-11+-blue.svg)](https://openjdk.java.net/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![GitHub Issues](https://img.shields.io/github/issues/dream11/shard-wizard)](https://github.com/dream11/shard-wizard/issues)
[![GitHub Stars](https://img.shields.io/github/stars/dream11/shard-wizard)](https://github.com/dream11/shard-wizard/stargazers)

A powerful, reactive Java library for managing database sharding in distributed systems with dynamic shard allocation, zero-downtime scaling, and comprehensive observability.

## 🎯 Overview

ShardWizard is designed to solve complex database sharding challenges in high-scale applications. Built on Vert.x, it provides a robust framework for managing multiple database shards with intelligent routing, automatic failover, and seamless scalability.

### Key Features

- **🔄 Dynamic Shard Management**: Add or remove shards without application downtime
- **🎯 Intelligent Routing**: Consistent hashing and custom routing strategies
- **🛡️ Fault Tolerance**: Built-in circuit breakers and automatic failover
- **📊 Multi-Database Support**: PostgreSQL, MySQL, DynamoDB, and S3 backends
- **📈 Observability**: Comprehensive metrics, tracing, and health monitoring
- **⚡ Reactive Architecture**: Non-blocking, asynchronous operations using RxJava
- **🏗️ Flexible Configuration**: Support for different shard configurations per entity
- **🔧 Easy Integration**: Simple DAO factory pattern for existing applications

### Use Cases

- **High-Traffic Applications**: Distribute load across multiple database instances
- **Multi-Tenant Systems**: Isolate tenant data on dedicated shards
- **Geographic Distribution**: Route requests to region-specific databases
- **Event-Based Systems**: Scale specific event types (e.g., IPL matches) on dedicated infrastructure
- **Microservices**: Manage data consistency across service boundaries

## 🏗️ Architecture

![System Diagram](img.png)

ShardWizard operates on a simple but powerful concept:
- **Entities** (e.g., tournaments, users) are mapped to one or more **Shards**
- **Route Keys** determine which shard handles specific requests
- **Default Shards** handle new entities automatically
- **Special Entities** can be configured on dedicated shards for isolation

## 📊 Database Support

### Shard Master Storage

ShardWizard supports multiple backends for storing shard management metadata:

| Storage Type | Status | Description | Use Case |
|-------------|--------|-------------|----------|
| **PostgreSQL** | ✅ Stable | Relational database with JSONB support | Production environments requiring ACID compliance |
| **MySQL** | ✅ Stable | Relational database with JSON support | MySQL-native applications |
| **DynamoDB** | ✅ Stable | NoSQL serverless database | Cloud-native, serverless deployments |
| **S3** | ✅ Stable | Object storage with JSON files | Configuration-as-code, GitOps workflows |

### Data Shard Support

Individual shards can use any of these database technologies:

| Database | Client Type | Status | Description |
|----------|-------------|--------|-------------|
| **PostgreSQL** | Vert.x Reactive | ✅ Stable | High-performance reactive client |
| **PostgreSQL** | HikariCP | ✅ Stable | Traditional connection pool based |
| **MySQL** | Vert.x Reactive | ✅ Stable | MySQL with reactive driver |
| **DynamoDB** | AWS SDK | ✅ Stable | NoSQL key-value and document database |

## 🚀 Quick Start

### Prerequisites

- Java 11 or higher
- Maven 3.6+
- One of the supported shard master storage systems

### Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.dream11</groupId>
    <artifactId>shard-wizard</artifactId>
    <version>x.y.z</version>
</dependency>
```

### Configuration

Create configuration file at `src/main/resources/config/shard-manager/default.conf`:

```hocon
sourceType = "POSTGRES"  # POSTGRES, MYSQL, DYNAMO, or S3
shardsRefreshSeconds = 60
serviceName = "my-service"

sources = {
  POSTGRES {
    type = "POSTGRES"
    writerHost = "localhost"
    readerHost = "localhost"
    port = 5432
    database = "shardmaster"
    username = "postgres"
    password = "password"
    
    databaseDefaultShardConfigMap = {
      POSTGRES {
        database = "app_shard"
        username = "app_user"
        password = "app_password"
        maxConnections = 10
        circuitBreaker {
          enabled = true
          failureRateThreshold = 50
          waitDurationInOpenState = 30000
        }
      }
    }
  }
}
```

## 🗄️ Shard Master Setup

### PostgreSQL Setup

```sql
-- Create shard management tables
CREATE TABLE ShardMaster (
    ShardId   SERIAL PRIMARY KEY,
    IsActive  BOOLEAN      NOT NULL DEFAULT true,
    IsDefault BOOLEAN      NOT NULL DEFAULT false,
    Details   varchar(512) NOT NULL,
    CreatedAt TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TIMESTAMP             DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE EntityShardMapping (
    EntityId  VARCHAR(255) PRIMARY KEY,
    ShardIds  JSONB     NOT NULL,
    CreatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TIMESTAMP          DEFAULT CURRENT_TIMESTAMP
);

-- Sample data
INSERT INTO ShardMaster (IsActive, IsDefault, Details)
VALUES (true, true, 
        '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5432, "readerHost": "localhost", "writerHost": "localhost", "maxConnections": 10}}');
```

### MySQL Setup

```sql
-- Create shard management tables
CREATE TABLE ShardMaster (
    ShardId   INT AUTO_INCREMENT PRIMARY KEY,
    IsActive  BOOLEAN  NOT NULL DEFAULT true,
    IsDefault BOOLEAN  NOT NULL DEFAULT false,
    Details   JSON     NOT NULL,
    CreatedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE EntityShardMapping (
    EntityId  VARCHAR(255) PRIMARY KEY,
    ShardIds  JSON     NOT NULL,
    CreatedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Sample data  
INSERT INTO ShardMaster (IsActive, IsDefault, Details)
VALUES (true, true, 
        '{"databaseType": "MYSQL", "shardConnectionParams": {"port": 3306, "readerHost": "localhost", "writerHost": "localhost", "maxConnections": 10}}');
```

### DynamoDB Setup

```bash
# Create ShardMaster table
aws dynamodb create-table \
    --table-name ShardMaster \
    --attribute-definitions AttributeName=shardId,AttributeType=N \
    --key-schema AttributeName=shardId,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

# Create EntityShardMapping table  
aws dynamodb create-table \
    --table-name EntityShardMapping \
    --attribute-definitions AttributeName=entityId,AttributeType=S \
    --key-schema AttributeName=entityId,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST
```

Sample DynamoDB shard configuration:
```json
{
  "shardId": 1,
  "details": {
    "databaseType": "POSTGRES",
    "shardConnectionParams": {
      "writerHost": "localhost",
      "readerHost": "localhost", 
      "port": 5432,
      "maxConnections": 10
    }
  },
  "default": true,
  "active": true
}
```

### S3 Setup

Create JSON files in your S3 bucket:

**shard-master.json:**
```json
[
  {
    "shardId": 1,
    "details": {
      "databaseType": "POSTGRES",
      "shardConnectionParams": {
        "writerHost": "localhost",
        "readerHost": "localhost",
        "port": 5432,
        "maxConnections": 10
      }
    },
    "default": true,
    "active": true
  }
]
```

**entity-mappings/entity_001.json:**
```json
{
  "entityId": "tournament_001",
  "shardIds": [1, 2]
}
```

Configuration for S3:
```hocon
sources = {
  S3 {
    type = "S3"
    bucketName = "my-shard-config-bucket"
    shardMasterFilePath = "shard-manager/shard-master.json"
    entityShardMappingFolderPath = "shard-manager/entity-mappings/"
    region = "us-east-1"
    accessKey = ""  # Use IAM roles
    secretKey = ""  # Use IAM roles
  }
}
```

## 💻 Implementation

### 1. Define Your DAO Interface

```java
public interface OrderDao {
    Single<String> create(OrderDto orderDto);
    Single<OrderDto> get(String orderId);
    Single<Boolean> update(OrderDto orderDto);
}
```

### 2. Implement DAO Factory

```java
public class OrderDaoFactory extends DaoFactory<OrderDao> {

    public OrderDaoFactory(Vertx vertx) {
        super(vertx);
    }

    @Override
    protected OrderDao getDaoImplFromType(ShardDetails shardDetails) {
        DatabaseType databaseType = shardDetails.getShardConfig().getDatabaseType();
        switch (databaseType) {
            case POSTGRES:
                return new PostgresOrderDaoImpl(vertx, shardDetails);
            case MYSQL:
                return new MySqlOrderDaoImpl(vertx, shardDetails);
            case DYNAMO:
                return new DynamoOrderDaoImpl(vertx, shardDetails);
            default:
                throw new IllegalArgumentException("Unsupported database type: " + databaseType);
        }
    }

    @Override
    protected long getShardIdFromPrimaryKey(String primaryKey) {
        // Extract shard ID from format "ORD-001-uuid"
        return Long.parseLong(primaryKey.split("-")[1]);
    }
}
```

### 3. Implement DAO

```java
public class PostgresOrderDaoImpl extends PostgresBaseDao implements OrderDao {

    public PostgresOrderDaoImpl(Vertx vertx, ShardDetails shardDetails) {
        super(vertx, shardDetails);
    }

    @Override
    public Single<String> create(OrderDto orderDto) {
        String orderId = generateOrderId();
        return postgresVertxClient
            .rxExecutePreparedQuery(RdsCluster.WRITER,
                "INSERT INTO orders(order_id, order_name, user_id) VALUES($1, $2, $3)",
                Tuple.of(orderId, orderDto.getOrderName(), orderDto.getUserId()))
            .map(result -> orderId);
    }

    @Override
    public Single<OrderDto> get(String orderId) {
        return postgresVertxClient
            .rxExecutePreparedQuery(RdsCluster.READER,
                "SELECT * FROM orders WHERE order_id = $1",
                Tuple.of(orderId))
            .map(this::mapToOrderDto);
    }
}
```

### 4. Bootstrap Application

```java
public class Application {
    
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        OrderDaoFactory factory = new OrderDaoFactory(vertx);
        
        factory.rxBootstrap()
            .subscribe(
                () -> log.info("ShardWizard started successfully"),
                error -> log.error("Failed to start", error)
            );
    }
}
```

### 5. Usage in Service

```java
@Service
public class OrderService {
    
    @Inject OrderDaoFactory orderDaoFactory;
    
    public Single<String> createOrder(String entityId, OrderDto orderDto) {
        return orderDaoFactory
            .rxGetOrCreateEntityShardDao(entityId, orderDto.getUserId())
            .flatMap(dao -> dao.create(orderDto));
    }
    
    public Single<OrderDto> getOrder(String orderId) {
        return orderDaoFactory
            .rxGetDaoInstanceByPrimaryKey(orderId)
            .flatMap(dao -> dao.get(orderId));
    }
}
```

## 📊 Observability

### Monitoring Providers

| Provider | Status | Description |
|----------|--------|-------------|
| **Datadog** | ✅ Stable | StatsD-based metrics and events |
| **New Relic** | ✅ Stable | APM and infrastructure monitoring |
| **Custom** | ✅ Stable | Implement your own `ObservabilityAdapter` |
| **No-Op** | ✅ Stable | Disabled monitoring for development |

### Basic Metrics

```java
// Record database events
DatabaseEventRecorder.getInstance()
    .recordSuccess(Constants.Metric.DB_QUERY, RdsCluster.WRITER, "INSERT INTO orders");

// Custom metrics with execution time
DatabaseEventRecorder.getInstance().recordEvent(
    AdvancedDatabaseEventBuilder.create()
        .eventName(Constants.Metric.DB_QUERY)
        .executionTime(150L)
        .addCustomTag("shard_id", "3")
        .build()
);
```

### Configure Monitoring

```java
ObservabilityConfig config = ObservabilityConfig.builder()
    .provider("datadog")  // datadog, newrelic, or noop
    .serviceName("my-service")
    .datadogConfig(DatadogConfig.builder()
        .host("localhost")
        .port(8125)
        .build())
    .build();

ObservabilityServiceFactory.setConfiguration(config);
```

## 🔧 Advanced Features

### Custom Routing

```java
public class CustomRouter implements ShardRouter {
    @Override
    public long getRoutedShardId(String routeKey) {
        // Your custom routing logic
        return routeKey.hashCode() % shardIds.size();
    }
}

// Use in DAO factory
@Override
protected ShardRouter getShardRouter() {
    return new CustomRouter();
}
```

### Dynamic Shard Management

```java
// Add new shard at runtime
ShardManagerClient shardManager = ShardManagerClient.create(vertx);

ShardConfig newShard = ShardConfig.builder()
    .databaseType(DatabaseType.POSTGRES)
    .shardConnectionParams(/* connection params */)
    .build();

shardManager.rxRegisterNewShard(true, newShard)
    .subscribe(shardDetails -> log.info("New shard added: {}", shardDetails.getShardId()));
```

## 🤝 Contributing

We welcome contributions! Here's how you can help:

1. **🐛 Report Bugs**: Use our [issue tracker](https://github.com/dream11/shard-wizard/issues/new?template=bug_report.md)
2. **💡 Request Features**: Submit feature requests [here](https://github.com/dream11/shard-wizard/issues/new?template=feature_request.md)
3. **📝 Improve Documentation**: Help us improve our docs
4. **🔧 Submit Code**: Fork the repo and submit a pull request

### Development Setup

1. Fork the repository
2. Clone your fork: `git clone https://github.com/yourusername/shard-wizard.git`
3. Install dependencies: `mvn clean install`
4. Run tests: `mvn test`
5. Make your changes and submit a pull request

### Code Style

- Follow [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)
- Ensure all tests pass
- Add tests for new features
- Update documentation as needed

## 📋 Requirements

- **Java**: 11 or higher
- **Build Tool**: Maven 3.6+
- **Runtime**: Vert.x 3.9.2
- **Dependencies**: RxJava 2, HikariCP, AWS SDK

## 📄 License

This project is licensed under the Apache License 2.0 - see the full license text at [Apache 2.0](https://opensource.org/licenses/Apache-2.0).

```
Copyright 2024 Dream11

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## 🆘 Support

- **📋 Issues**: [GitHub Issues](https://github.com/dream11/shard-wizard/issues)

## 📈 Project Status

- **Development Status**: Active development
- **Production Ready**: ✅ Yes
- **Stability**: Stable
- **Performance**: Production-tested with millions of requests daily

## 🏆 Acknowledgments

- **Dream11 Engineering Team**: Core development and maintenance
- **Vert.x Community**: For the excellent reactive toolkit
- **RxJava Team**: For reactive extensions for the JVM
- **Contributors**: Thank you to all our [contributors](https://github.com/dream11/shard-wizard/contributors)

---

**Built with ❤️ by [Dream11](https://www.dream11.com) Engineering Team**

Used in production to handle millions of requests daily across multiple database shards.
