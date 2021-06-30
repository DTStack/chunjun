-- curl -XPOST 'localhost:9200/teachers/1' -H "Content-Type: application/json" -d '{
-- "id": "100",
-- "phone": "2345678765",
-- "qq": "7576457",
-- "wechat": "这是wechat",
-- "income": "1324.13",
-- "birthday": "2020-07-30 10:08:22",
-- "today": "2020-07-30",
-- "timecurrent": "12:08:22"
-- }'

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
    'connector' ='elasticsearch7-x',
    'hosts' ='localhost:9200',
    'index' ='mowen_target');


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
   'connector' ='elasticsearch7-x'
   ,'hosts' ='localhost:9200',
    'index' ='students_4'
    );


INSERT INTO es7_sink
SELECT *
FROM es7_source;
