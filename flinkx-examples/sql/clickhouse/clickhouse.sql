
CREATE TABLE source (
  id   INT,
  name STRING
) WITH (
  'connector' = 'kafka-x',
  'topic' = 'test',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'dodge',
  'format' = 'json'
);

CREATE TABLE side (
  id INT,
  name VARCHAR,
  create_time TIMESTAMP,
  test1 SMALLINT,
  test2 BIGINT,
  afloat FLOAT,
  afloat2 DOUBLE,
  is_delete TINYINT,
  create_date DATE
) WITH (
  'connector' = 'clickhouse-x',
  'url' = 'jdbc:clickhouse://localhost:8123/default',
  'table-name' = 'sql_side_table',
  'username' = 'default',
  'password' = 'b6rCe7ZV',
  'lookup.cache-type' = 'lru'
);

CREATE TABLE sink (
  id INT,
  name VARCHAR,
  create_time TIMESTAMP,
  test1 SMALLINT,
  test2 BIGINT,
  afloat FLOAT,
  afloat2 DOUBLE,
  is_delete TINYINT,
  create_date DATE
) WITH (
  'connector' = 'clickhouse-x',
  'url' = 'jdbc:clickhouse://localhost:8123/default',
  'table-name' = 'sql_sink_table',
  'username' = 'default',
  'password' = 'b6rCe7ZV',
  'sink.buffer-flush.max-rows' = '1',
  'sink.all-replace' = 'true'
);

INSERT INTO sink
  SELECT
    s1.id AS id,
    s1.name AS name,
    s2.create_time AS create_time,
    s2.test1 AS test1,
    s2.test2 AS test2,
    s2.afloat AS afloat,
    s2.afloat2 AS afloat2,
    s2.is_delete AS is_delete,
    s2.create_date AS create_date
  FROM source s1
  JOIN side s2
  ON s1.id = s2.id


