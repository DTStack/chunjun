CREATE TABLE source
(
    id   INT,
    name STRING
) WITH (
      'connector' = 'rabbitmq-x',
      'host' = 'hostname',
      'username' = 'root',
      'password' = 'root',
      'virtual-host' = '/',
      'queue-name' = 'first2',
      'format' = 'json'
      );

CREATE TABLE sink
(
    id   INT,
    name STRING
) WITH (
      'connector' = 'stream-x',
      'print' = 'true'
      );

insert into sink
select *
from source;
