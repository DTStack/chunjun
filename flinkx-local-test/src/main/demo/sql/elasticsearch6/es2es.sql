CREATE TABLE es6_source(
    id int
  , phone bigint
  , qq varchar
  , wechat varchar
  , income decimal(10,2)
  , birthday timestamp
  , today date
  , timecurrent time )

WITH(
    'connector' ='elasticsearch6-x',
    'hosts' ='localhost:9200',
    'index' ='mowen',
    'document-type' = '_doc');


CREATE TABLE es6_sink(
   id int
  , phone bigint
  , qq varchar
  , wechat varchar
  , income decimal(10,2)
  , birthday timestamp
  , today date
  , timecurrent time )

WITH(
   'connector' ='stream-x'
--    ,'hosts' ='localhost:9200',
--     'index' ='testdate15',
--     'document-type' = '_doc'
    );


INSERT INTO es6_sink
SELECT *
FROM es6_source;
