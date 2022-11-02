
CREATE TABLE test
(
    id           bigint,
    deployment_pictures ARRAY<varchar>,
    deployment_images MAP<String,String>,
    deployment_contacts ROW(Name varchar,Contact varchar),
    origin_id     varchar
) WITH (
      'connector' = 'postgresql-x',
      'url' = 'jdbc:postgresql://localhost:5432/visual_test1',
      'table-name' = 'test',
      'username' = 'pg',
      'password' = 'pg'
      );

CREATE TABLE sink(
                     id           bigint,
                     deployment_pictures ARRAY<varchar>,
                     deployment_images MAP<String,String>,
                     deployment_contacts ROW(Name varchar,Contact varchar),
                     origin_id     varchar
)WITH(
     'connector' = 'stream-x',
     'print' = 'true'
     );

insert into sink
select * from test;
