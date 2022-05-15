CREATE TABLE source_pg
(
    val_character_varying STRING,
    val_varchar           STRING,
    val_character         STRING,
    val_char              STRING,
    val_text              STRING,
    val_name              STRING,
    val_bytea             BYTES,
    val_timestamp         TIMESTAMP,
    val_timestamptz       TIMESTAMP,
    val_date              DATE,
    val_time              TIME,
    val_timetz            TIME,
    val_boolean           BOOLEAN
) WITH (
    'connector' = 'postgresql-x',
    'url' = 'jdbc:postgresql://localhost:5432/dev',
    'table-name' = 'dim_without_numeric',
    'username' = 'postgres',
    'password' = 'root',
    'scan.fetch-size' = '2',
    'scan.query-timeout' = '10'
);

CREATE TABLE sink_stream (
     val_character_varying STRING,
     val_varchar           STRING,
     val_character         STRING,
     val_char              STRING,
     val_text              STRING,
     val_name              STRING,
     val_bytea             BYTES,
     val_timestamp         TIMESTAMP,
     val_timestamptz       TIMESTAMP,
     val_date              DATE,
     val_time              TIME,
     val_timetz            TIME,
     val_boolean           BOOLEAN
) WITH (
    'connector' = 'stream-x'
);

INSERT INTO sink_stream
SELECT * FROM source_pg;
