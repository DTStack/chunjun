CREATE
CATALOG my_catalog WITH (
    'type'='paimon',
    'metastore' = 'filesystem',
    'warehouse'='file:/tmp/paimon'
);

USE CATALOG my_catalog;

-- create a word count table
CREATE TABLE if not exists word_count
(
    word STRING PRIMARY KEY NOT ENFORCED,
    cnt  BIGINT
);

CREATE
TEMPORARY TABLE sink (
    word STRING,
   cnt  BIGINT
) WITH (
    'connector' = 'stream-x'
);

insert into sink  SELECT word, cnt FROM word_count ;
