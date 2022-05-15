CREATE TABLE FILE_SOURCE (
    C_ID INT,
    C_BOOLEAN BOOLEAN,
    C_TINYINT TINYINT,
    C_SMALLINT SMALLINT,
    C_INT INT,
    C_BIGINT BIGINT,
    C_FLOAT FLOAT,
    C_DOUBLE DOUBLE,
    C_DECIMAL DECIMAL,
    C_STRING STRING,
    C_VARCHAR VARCHAR(255),
    C_CHAR CHAR(255),
    C_TIMESTAMP TIMESTAMP,
    C_DATE DATE
) WITH (
    'connector' = 'ftp-x',
    'path' = '/data/sftp/chunjun/sql/source/data.csv',
    'protocol' = 'sftp',
    'host' = 'localhost',
    'username' = 'root',
    'password' = 'xxxxxx',
    'format' = 'csv'
);

CREATE TABLE SINK (
    C_ID INT,
    C_BOOLEAN BOOLEAN,
    C_TINYINT TINYINT,
    C_SMALLINT SMALLINT,
    C_INT INT,
    C_BIGINT BIGINT,
    C_FLOAT FLOAT,
    C_DOUBLE DOUBLE,
    C_DECIMAL DECIMAL,
    C_STRING STRING,
    C_VARCHAR VARCHAR(255),
    C_CHAR CHAR(255),
    C_TIMESTAMP TIMESTAMP,
    C_DATE DATE
) WITH (
    'connector' = 'stream-x',
    'print' = 'true'
);

INSERT INTO SINK
SELECT *
FROM FILE_SOURCE;
