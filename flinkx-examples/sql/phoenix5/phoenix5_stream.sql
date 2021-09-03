CREATE TABLE source (
    id bigint primary key,
    NAME varchar
) WITH (
    'connector' = 'phoenix5-x',
    'username' = '',
    'password' = '',
    'url' = 'jdbc:phoenix:flinkx1,flinkx2,flinkx3:2181',
    'table-name' = 't1'
);

CREATE TABLE sink (
    id bigint,
    name varchar
) WITH ('connector' = 'stream-x');

insert into sink
select *
from source u;
