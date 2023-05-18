CREATE TABLE source
(
    id   int,
    name string
) with (
      'connector' = 'stream-x',
      'number-of-rows' = '10'
      );

CREATE TABLE sink
(
    id   int,
    name string


) with (
      'connector' = 'starrocks-x',
      'url' = 'jdbc:mysql://node1:9030',
      'fe-nodes' = 'node1:8030;node2:8030;node3:8030',
      'schema-name' = 'test',
      'table-name' = 'sink',
      'username' = 'root',
      'password' = ''
      );

insert into sink
select *
from source;
