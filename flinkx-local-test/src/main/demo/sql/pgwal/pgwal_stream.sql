CREATE TABLE source
(
    id             bigint,
    col_bit        boolean
) WITH (
      'connector' = 'PGWalFactory'
      ,'username' = 'postgres'
      ,'password' = 'postgres'
      ,'url' = 'jdbc:postgresql://localhost:5432/postgres?useUnicode=true&characterEncoding=utf8'
      ,'table-name' = 'public.test_pg'
      );

CREATE TABLE sink
(
    id             bigint,
    col_bit        boolean
) WITH ('connector' = 'stream-x');

insert into sink
select *
from source u;
