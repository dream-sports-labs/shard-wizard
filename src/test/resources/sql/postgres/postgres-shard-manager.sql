CREATE TABLE ShardMaster
(
    ShardId   SERIAL PRIMARY KEY,
    IsActive  BOOLEAN      NOT NULL DEFAULT true,
    IsDefault BOOLEAN      NOT NULL DEFAULT false,
    Details   varchar(512) NOT NULL,
    CreatedAt TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TIMESTAMP             DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE EntityShardMapping
(
    EntityId  VARCHAR(255) PRIMARY KEY,
    ShardIds  JSONB     NOT NULL,
    CreatedAt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UpdatedAt TIMESTAMP          DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO ShardMaster (ShardId, IsActive, IsDefault, Details, CreatedAt, UpdatedAt)
VALUES (1, true, false,
        '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5433, "readerHost": "localhost","writerHost":"localhost", "maxConnections": 5, "maxWaitQueueSize": 50, "connectionTimeoutMs": 500}}',
        '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');
INSERT INTO ShardMaster (ShardId, IsActive, IsDefault, Details, CreatedAt, UpdatedAt)
VALUES (2, true, false,
        '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5434, "readerHost": "localhost","writerHost":"localhost", "maxConnections": 5, "maxWaitQueueSize": 50, "connectionTimeoutMs": 500}}',
        '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');
INSERT INTO ShardMaster (ShardId, IsActive, IsDefault, Details, CreatedAt, UpdatedAt)
VALUES (3, true, true,
        '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5435, "readerHost": "localhost","writerHost":"localhost", "maxConnections": 5, "maxWaitQueueSize": 50, "connectionTimeoutMs": 500}}',
        '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');
INSERT INTO ShardMaster (ShardId, IsActive, IsDefault, Details, CreatedAt, UpdatedAt)
VALUES (4, true, true,
        '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5436, "readerHost": "localhost","writerHost":"localhost", "maxConnections": 5, "maxWaitQueueSize": 50, "connectionTimeoutMs": 500}}',
        '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');
INSERT INTO ShardMaster (ShardId, IsActive, IsDefault, Details, CreatedAt, UpdatedAt)
VALUES (5, false, false,
        '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5437, "readerHost": "localhost","writerHost":"localhost", "maxConnections": 5, "maxWaitQueueSize": 50, "connectionTimeoutMs": 500, "username": "postgres", "password": "postgres", "database": "postgres"}}',
        '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');

INSERT INTO ShardMaster (ShardId, IsActive, IsDefault, Details, CreatedAt, UpdatedAt)
 VALUES (6, true, false,
        '{"databaseType": "MYSQL", "shardConnectionParams": {"port": 5438, "readerHost": "localhost","writerHost":"localhost", "maxConnections": 5, "maxWaitQueueSize": 50, "connectionTimeoutMs": 500, "username": "mysql", "password": "mysql", "database": "mysql"}}',
        '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');



INSERT INTO EntityShardMapping (entityid, shardids)
VALUES
    ('2222', '[1,2]'),
    ('3333', '[5]'),
    ('4446', '[1,2]'),
    ('1009', '[3,4]'),
    ('1012', '[1,2]'),
    ('1024', '[1,6]');
