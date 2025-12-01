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
TEMPORARY TABLE word_table (
    word STRING
) WITH (
    'connector' = 'datagen',
    'fields.word.length' = '1'
);



SET execution.checkpointing.interval=10000;

INSERT INTO word_count
SELECT word, COUNT(*)
FROM word_table
GROUP BY word;
