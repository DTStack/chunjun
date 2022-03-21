-- curl -XPOST 'localhost:9200/teachers/1' -H "Content-Type: application/json" -d '{
-- "id": "100",
-- "phone": "2345678765",
-- "qq": "7576457",
-- "wechat": "这是wechat",
-- "income": "1324.13",
-- "birthday": "2020-07-30 10:08:22",
-- "today": "2020-07-30",
-- "timecurrent": "12:08:22"
-- }'
ADD FILE WITH /data/sftp/ssl_DsCenter_17559/ca.crt;
CREATE TABLE es7_source
(
    id   int,
    name varchar,
    age  varchar,
    text varchar
)
    WITH (
        'connector' = 'elasticsearch7-x',
        'hosts' ='127.0.0.1:9200',
        'username' = 'elastic',
        'password' = 'elastic',
        'index' = 'today_001',
        'client.connect-timeout' = '10000',
        'security.ssl-keystore-file'='ca.crt',
        'security.ssl-keystore-password'='',
        'security.ssl-type'='ca'
        );


CREATE TABLE es7_sink
(
    id   int,
    name varchar,
    age  varchar,
    text varchar
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


INSERT INTO es7_sink
SELECT *
FROM es7_source;
