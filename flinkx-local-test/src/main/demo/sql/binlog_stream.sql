CREATE TABLE source (
    id  int
    , name varchar
    , user_id int
) WITH  (
  'connector' = 'binlog-x'
  ,'username' = 'root'
  ,'password' = 'root'
  ,'cat' = 'insert,delete,update'
  ,'jdbcUrl' = 'jdbc:mysql://localhost:3308/tudou?useSSL=false'
  ,'host' = 'kudu3'
  ,'port' = '3308'
--   ,'journalName' = 'mysql-bin.000001'
  ,'table' = 'kudu'
  ,'timestamp-format.standard' = 'SQL'
);

CREATE TABLE sink (
    id  int
    , user_id int
    ,name varchar
) WITH (
    'connector' = 'stream-x'
);

insert into sink
  select
    u.name
    , u.id
    , u.user_id
  from source u ;