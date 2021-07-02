CREATE TABLE es7_source(
    id int
  , phone bigint
  , qq varchar
  , wechat varchar
  , income decimal(10,6)
  , birthday timestamp
  , today date
  , timecurrent time )
WITH(
    'connector' ='elasticsearch5-x',
    'hosts' ='localhost:9300',
    'index' ='mowen1',
    'cluster' = 'elasticsearch',
    'document-type' = 'schema1');


CREATE TABLE es7_sink(
    id int
  , phone bigint
  , qq varchar
  , wechat varchar
  , income decimal(10,6)
  , birthday timestamp
  , today date
  , timecurrent time )
WITH(
   'connector' ='elasticsearch5-x'
   ,'hosts' ='localhost:9300',
    'index' ='students_4',
    'cluster' = 'elasticsearch',
    'document-type' = 'schema12'
    );


INSERT INTO es7_sink
SELECT *
FROM es7_source;
