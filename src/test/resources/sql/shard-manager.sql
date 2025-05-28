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

INSERT INTO shardmaster (shardid, isactive, isdefault, details, createdat, updatedat)
VALUES (1, true, false,
        '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5433, "readerHost": "localhost","writerHost":"localhost", "maxConnections": 5, "maxWaitQueueSize": 50, "connectionTimeoutMs": 500}}',
        '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');
INSERT INTO shardmaster (shardid, isactive, isdefault, details, createdat, updatedat)
VALUES (2, true, false,
        '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5434, "readerHost": "localhost","writerHost":"localhost", "maxConnections": 5, "maxWaitQueueSize": 50, "connectionTimeoutMs": 500}}',
        '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');
INSERT INTO shardmaster (shardid, isactive, isdefault, details, createdat, updatedat)
VALUES (3, true, true,
        '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5435, "readerHost": "localhost","writerHost":"localhost", "maxConnections": 5, "maxWaitQueueSize": 50, "connectionTimeoutMs": 500}}',
        '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');
INSERT INTO shardmaster (shardid, isactive, isdefault, details, createdat, updatedat)
VALUES (4, true, true,
        '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5436, "readerHost": "localhost","writerHost":"localhost", "maxConnections": 5, "maxWaitQueueSize": 50, "connectionTimeoutMs": 500}}',
        '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');
INSERT INTO shardmaster (shardid, isactive, isdefault, details, createdat, updatedat)
VALUES (5, false, false,
        '{"databaseType": "POSTGRES", "shardConnectionParams": {"port": 5437, "readerHost": "localhost","writerHost":"localhost", "maxConnections": 5, "maxWaitQueueSize": 50, "connectionTimeoutMs": 500, "username": "postgres", "password": "postgres", "database": "postgres"}}',
        '2024-08-14 03:43:06.220381', '2024-08-14 03:43:06.220381');


INSERT INTO EntityShardMapping (entityid, shardids)
VALUES ('2222', '[
  1,
  2
]'),
       ('3333', '[
         5
       ]'),
       ('4446', '[
         1,
         2
       ]'),
       ('1009', '[
         3,
         4
       ]'),
       ('1012', '[
         1,
         2
       ]');