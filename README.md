# ShardWizard

## Overview

ShardWizard outlines an approach that effectively supports key requirements for system scalability
and flexibility. It enables the isolation and hosting of major matches, such as IPL, in dedicated
shards. The solution is designed to accommodate increased demand by seamlessly scaling the system
through the addition of shards.
Furthermore, it provides the flexibility to incorporate any type of shard during scaling and allows
for the removal of shards without causing downtime.

The library (written in Vert.x) is designed to manage DAOs of shards for entities
(for e.g. rounds). The approach involves mapping an `entity -> list of shardIds`. Each
entity can be hosted on a single shard or distributed across multiple shards.
<br><br>
When multiple shards are mapped to an entity, `routeKey` is used to identify which
shard the request will route to. The default
routing mechanism is [Consistent Hashing](https://www.geeksforgeeks.org/consistent-hashing/) (which
can be modified; see [Miscellaneous](#miscellaneous) section). This
approach helps reduce the blast radius in case
of a failure.

#### Default Shards

Clients need to configure default shards, which will map to any new entity by default. Multiple
default
shards can be set up, and requests for that entity will be distributed across these default
shards based on the `routeKey`.

#### Special Entities

Special entities, for e.g. IPL round, can be configured to be hosted on different shards from the
default ones.
However, this configuration must be set up before the round is opened or before the first request
for that round is received. Once an entity is assigned to shard(s), the configuration
cannot be changed. This feature allows you to host special rounds like IPL on a separate stack.

#### Experimentation and Database Configuration

This architecture also facilitates experimentation. By configuring certain entities to be hosted on
different shards, you can choose which database powers each shard. Additionally, you can attach
specific configuration parameters to each shard.

#### Dynamic Shard Management

Shards can be discarded once the data on that shard is no longer needed. You can simply
mark the shard as inactive in the configuration. Clients do not need to restart their services when
a new shard is added. The library will dynamically refresh connections and close connections to
shards that are no longer in use.
There is no downtime in configuring a new shard

### System Diagram

![System Diagram](img.png)

## How to use?

Follow these steps to use this client in your project:

1. **Adding Dependency**
    ```xml
    <dependency>
        <groupId>com.dream11</groupId>
        <artifactId>shard-wizard</artifactId>
        <version>x.y.z</version>
    </dependency>
    ```

2. **Building the Project**

   Install all project dependencies and build the project using Maven:

    ```bash
    mvn clean install
    ```

3. **Create ShardManger config**

   ShardManager client requires a config file to be present in the
   application path `.../resources/config/shard-manager/<env>.conf` having this format: <br>

    <details>
      <summary>ShardManager config [click to expand]</summary>

      ```lombok.config
    #Defines from which source you want to read the shard-manager data
    sourceType = "S3" or "POSTGRES" or "MYSQL"

    #[OPTIONAL] Time in seconds after which the shard manager data will refresh the active shards. Default value is 60 seconds when not provided.
    shardsRefreshSeconds = 60
    
    #Connections Map 
    #Which connection parameters to be picked are based on sourceType
    sources = {
      S3 {
        #Type of source where shard manager data is stored
        type = "S3"
        #Bucket Name in S3 where shard manager data is stored
        bucketName = "d11-contest-orders-load"
        #File Name where shardMaster data is stored
        shardMasterFilePath = "orders_shard_manager/shardmaster.json"
        #Folder where entity shard mapping files will be stored
        entityShardMappingFolderPath = "orders_shard_manager/entity_shard_mapping/"
        #Region    
        region = "us-east-1"
        #AcessKey to access S3 bucket, If IAM roles are setup use accesskey as empty string
        accessKey = ""
        #SecretKet to access S3 bucket. If IAM roles are setup use secretkey as empty string
        secretKey = ""
        #Default shard config. 
        #These connection paramters will be used if not provided in ShardMaster table "Details" column  
        databaseDefaultShardConfigMap = {
          POSTGRES {
            port = 5432
            database = "postgres"
            username = "dbUsername"
            password = "test"
            maxConnections = 5
          }
          MYSQL {
            port = 5435
            database = "mysql"
            username = "dbUsername"
            password = "test"
            maxConnections = 5
          }
        }   
      }
      POSTGRES {
        #Type of source where shard manager data is stored
        type = "POSTGRES"
        #Host of the writer shard where shard manager data is stored
        writerHost = "localhost"
        #Host of the reader shard where shard manager data is stored
        readerHost = "localhost"
        #Shard manager port
        port = 5432
        #Shard manager databaseName
        database = "postgres"
        #Shard manager database password
        password = "test"
        #Default shard config. 
        #These connection paramters will be used if not provided in ShardMaster table "Details" column  
        databaseDefaultShardConfigMap = {
          POSTGRES {
            port = 5432
            database = "postgres"
            username = "dbUsername"
            password = "test"
            maxConnections = 5
          }
          MYSQL {
            port = 5435
            database = "mysql"
            username = "dbUsername"
            password = "test"
            maxConnections = 5
          }
        } 
      }
    }
    ```
    </details>
    <br>

4. **Create Shard Management tables**

   Create two tables in the database where the shard manager data will be stored. <br><br>
    <details>
      <summary>For MySQL [click to expand]</summary>

    ```mysql
    CREATE TABLE ShardMaster
    (
        ShardId   INT AUTO_INCREMENT PRIMARY KEY,
        IsActive  BOOLEAN  NOT NULL DEFAULT true,
        IsDefault BOOLEAN  NOT NULL DEFAULT false,
        Details   JSON     NOT NULL,
        CreatedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UpdatedAt DATETIME          DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
    
    CREATE TABLE EntityShardMapping
    (
        EntityId  VARCHAR(255) PRIMARY KEY,
        ShardIds  JSON     NOT NULL,
        CreatedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UpdatedAt DATETIME          DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
    ```

   Sample shard with ShardId = 1

    ```mysql
    INSERT INTO public.shardmaster (ShardId, IsActive, IsDefault, Details)
    VALUES (1, true, true,
            '{"databaseType": "MYSQL", "shardConnectionParams": {"port": 3306, "readerHost": "route53-of-reader.local", "writerHost": "route53-of-writer.local","database":"mydb", "maxConnections": 5,"username":"dbUsername","password":"dbPassword"}}');
    ```

   **Note:** `shardConnectionParams` can be `{}`, in that case it will use the application supplied
   parameters present in the ShardManager config file.

    </details>

    <details>
      <summary>For PostgreSQL [click to expand]</summary>

    ```sql
    CREATE TABLE ShardMaster
    (
        shardid   SERIAL PRIMARY KEY,
        isactive  BOOLEAN      NOT NULL DEFAULT true,
        isdefault BOOLEAN      NOT NULL DEFAULT false,
        details   varchar(512) NOT NULL,
        createdat TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updatedat TIMESTAMP             DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE EntityShardMapping
    (
        entityid  VARCHAR(255) PRIMARY KEY,
        shardids  JSONB     NOT NULL,
        createdat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updatedat TIMESTAMP          DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE OR REPLACE FUNCTION update_updated_at_column()
        RETURNS TRIGGER AS
    $$
    BEGIN
        NEW."updatedat" = NOW();
        RETURN NEW;
    END;
    $$ language 'plpgsql';
    
    
    CREATE TRIGGER update_shardmaster_updated_at
        BEFORE UPDATE
        ON "shardmaster"
        FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
    
    CREATE TRIGGER update_entityshardmapping_updated_at
        BEFORE UPDATE
        ON "entityshardmapping"
        FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
    ```

   Sample Shard with ShardId = 1

    ```postgresql
    INSERT INTO public.shardmaster
    (shardid, isactive, isdefault, details, createdat, updatedat)
    VALUES (1, true, true,
            '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5432, "readerHost": "localhost", "writerHost": "localhost", "maxConnections": 5}}',
            '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');
    ```

   **Note:** `shardConnectionParams` can be `{}`, in that case it will use the application supplied
   parameters present in the ShardManager config file.

    </details>
    <br>
    Sample shard details in JSON:

    ```json
    {
      "databaseType": "MYSQL",
      "shardConnectionParams": {
        "port": 3306,
        "readerHost": "route53-of-reader.local",
        "writerHost": "route53-of-writer.local",
        "database": "mydb",
        "maxConnections": 5,
        "username": "dbUsername",
        "password": "dbPassword"
      }
    }
    ```

## Usage in Application

### 1. Extend `DaoFactory<T>`

This factory is designed to facilitate the creation and management of Data Access Object (DAO)
instances of type `T`.
Its responsibilities include not only the instantiation of DAOs but also handling their connections
and disconnections from database shards.

The factory utilizes an instance of internally created `ShardManagerClient` to retrieve necessary
shard-related information

Extend `DaoFactory<T>` with your specific DAO type `T`. You need to implement these two abstract
functions for it to work:

```java
protected abstract T getDaoImplFromType(ShardDetails shardDetails);

protected abstract long getShardIdFromPrimaryKey(String primaryKey);
```

- `getDaoImplFromType` is used to return the DAO instance based on the shardDetails argument
  provided.
- `getShardIdFromPrimaryKey` is used to return the shardId based on the primary key. In order to use
  the factory's function `rxGetDaoInstanceByPrimaryKey` you need to implement this function. This
  function helps in extracting shardId from PrimaryKey which is used to get the DAO instance.

For more info on functions provided by DaoFactory, refer the
interface [IDaoFactory.java](src%2Fmain%2Fjava%2Fcom%2Fdream11%2Fshardwizard%2Fdao%2FIDaoFactory.java)

<br>

<details>
      <summary>  Sample DaoFactory implementation [Click to Expand]  </summary>

```java
public class OrderDaoFactory extends DaoFactory<OrderDao> {

  public OrderDaoFactory(Vertx vertx) {
    super(vertx);
  }

  @Override
  protected OrderDao getDaoImplFromType(ShardDetails shardDetails) {
    switch (shardDetails.getShardConfig().getDatabaseType()) {
      case MYSQL:
        return new MysqlOrderDaoImpl(vertx, shardDetails);
      case POSTGRES:
        return new PostgresOrderDaoImpl(vertx, shardDetails);
      default:
        throw new IllegalArgumentException("Invalid database type");
    }
  }

  @Override
  protected long getShardIdFromPrimaryKey(String primaryKey) {
    return Long.parseLong(primaryKey.substring(3, 6));
  }
}
```

</details>
  <br>

### 2. Bootstrapping

Create instance of DaoFactory and bootstrap it in the main application class.

```java

@Override
protected Completable rxDeployVerticles(){
    return Completable.defer(
    ()->{
    OrderDaoFactory orderDaoFactory=new OrderDaoFactory(vertx);
    SharedDataUtils.setInstance(this.vertx,orderDaoFactory);
    return orderDaoFactory.rxBootstrap();
    })
    .andThen(super.rxDeployVerticles());
    }
```

```java
// Bind the factory in MainModule class
bind(OrderDaoFactory.class)
  .toProvider(() -> SharedDataUtils.getInstance(vertx, OrderDaoFactory.class))
  .in(Singleton.class);
```

### 3. Usage in a service class

In the below examples, OrderDaoFactory is the factory class that extends DaoBuilderFactory<OrderDao>
and is responsible for
creating OrderDao instances.

```java

@Inject
OrderDaoFactory orderDaoFactory;
```

```java
Single<OrderDao> dao=orderDaoFactory.rxGetOrCreateEntityShardDao("800013",60009001);
```

```java
Single<OrderDao> dao=orderDaoFactory.rxGetDaoInstanceByPrimaryKey(
    "ORD002-204e7daf-0fbb-47b7-8ba6-gdd671037f85");
```

## Sample implementations

### Sample DAO

```java
public interface OrderDao {

  void create(OrderDto orderDto);

  OrderDto get(String orderId);
}
```

### Sample DAO impl

```java

public class MySqlOrderDaoImpl extends MySqlVertxBaseDao implements OrderDao {

  public MysqlOrderDaoImpl(Vertx vertx, ShardDetails shardDetails) {
    super(vertx, shardDetails);
  }

  @Override
  public void create(OrderDto orderDto) {
    System.out.println("MysqlOrderDto created");
  }

  @Override
  public OrderDto get(String orderId) {
    System.out.println("Fetching Mysql orderDto");
    return new OrderDto();
  }
}
```

You can use these **pre-defined** BaseDaoAbstract class extensions

- [MySqlHikariBaseDao.java](src%2Fmain%2Fjava%2Fcom%2Fdream11%2Fshardwizard%2Fdao%2Fimpl%2FMySqlHikariBaseDao.java)
- [MySqlVertxBaseDao.java](src%2Fmain%2Fjava%2Fcom%2Fdream11%2Fshardwizard%2Fdao%2Fimpl%2FMySqlVertxBaseDao.java)
- [PostgresBaseDao.java](src%2Fmain%2Fjava%2Fcom%2Fdream11%2Fshardwizard%2Fdao%2Fimpl%2FPostgresBaseDao.java)

These have default implementation of `rxConnect()` and `rxCloseConnections()` and also expose
corresponding database clients through inheritance.

### Sample DaoFactory impl

```java
public class OrderDaoFactory extends DaoFactory<OrderDao> {

  public OrderDaoFactory(Vertx vertx) {
    super(vertx);
  }

  @Override
  protected OrderDao getDaoImplFromType(ShardDetails shardDetails) {
    switch (shardDetails.getShardConfig().getDatabaseType()) {
      case MYSQL:
        return new MysqlOrderDaoImpl(vertx, shardDetails);
      case POSTGRES:
        return new PostgresOrderDaoImpl(vertx, shardDetails);
      default:
        throw new IllegalArgumentException("Invalid database type");
    }
  }

  @Override
  protected long getShardIdFromPrimaryKey(String primaryKey) {
    return Long.parseLong(primaryKey.substring(3, 6));
  }
}
```

## Miscellaneous

### Changing the shard routing mechanism

To change the routing logic used, you need to create your custom router by implementing the
[ShardRouter](src%2Fmain%2Fjava%2Fcom%2Fdream11%2Fshardwizard%2Frouter%2FShardRouter.java)
interface.
After that, override the protected function in the DaoFactory<T> with your custom router. For
example:

```java

public class OrderDaoFactory extends DaoFactory<OrderDao> {

  public OrderDaoFactory(Vertx vertx) {
    super(vertx);
  }

  protected ShardRouter getShardRouter() {
    //supply your ShardRouter implementation here
    return new YourCustomRouter();
  }
}
```

You can also use a
predefined [ModuloRouter.java](src%2Fmain%2Fjava%2Fcom%2Fdream11%2Fshardwizard%2Frouter%2Fimpl%2FModuloRouter.java)
implementation as well. Just make sure routeKey is a number in order to use ModuloRouter.