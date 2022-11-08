
CREATE TABLE source
(
    id        INT,
    name      STRING,
    money     DECIMAL(32, 2),
    dateone   timestamp,
    age       bigint,
    datethree timestamp,
    datesix   timestamp(6),
    datenigth timestamp(9),
    dtdate    date,
    dttime    time,
    `row` ROW(id int,name varchar),
    plateText ARRAY<varchar>,
    pt MAP<varchar,int>
)
    WITH (
        'connector' = 'elasticsearch7-x',
        'hosts' = '127.0.0.1:9200',
        'username' = 'elastic',
        'password' = 'elastic',
        'index' = 'students_4',
        'client.connect-timeout' = '10000',
        'security.ssl-keystore-file'='ca.crt',
        'security.ssl-keystore-password'='',
        'security.ssl-type'='ca'
        );

CREATE TABLE sink
(
    id        INT,
    name      STRING,
    money     DECIMAL(32, 2),
    dateone   timestamp,
    age       bigint,
    datethree timestamp,
    datesix   timestamp(6),
    datenigth timestamp(9),
    dtdate    date,
    dttime    time,
    `row` ROW(id int,name varchar),
    plateText ARRAY<varchar>,
    pt MAP<varchar,int>
) WITH (
      'connector' = 'stream-x',
      'number-of-rows' = '10', -- 输入条数，默认无限
      'rows-per-second' = '1' -- 每秒输入条数，默认不限制
      );


insert into sink
select * from source;
