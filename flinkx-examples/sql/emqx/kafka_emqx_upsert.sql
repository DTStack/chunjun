-- {"id":100,"name":"lb james阿道夫","money":293.899778,"dateone":"2020-07-30 10:08:22","age":"33","datethree":"2020-07-30 10:08:22.123","datesix":"2020-07-30 10:08:22.123456","datenigth":"2020-07-30 10:08:22.123456789","dtdate":"2020-07-30","dttime":"10:08:22"}

-- {"id":100,"name":"lb james阿道夫","money":293.899778,"dateone":"2020-07-30 10:08:22","age":"33","datethree":"2020-07-30 10:08:22.123","datesix":"2020-07-30 10:08:22.123456","datenigth":"2020-07-30 10:08:22.123456789","dtdate":"2020-07-30","dttime":"10:08:22"}
CREATE TABLE source_ods_fact_user_ippv
(
    id        INT,
    name      STRING,
    money     decimal,
    dateone   timestamp,
    age       bigint,
    datethree timestamp,
    datesix   timestamp(6),
    datenigth timestamp(9),
    dtdate    date,
    dttime    time
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'da'
      ,'properties.bootstrap.servers' = 'localhost:9092'
      ,'properties.group.id' = 'luna_g'
      ,'scan.startup.mode' = 'earliest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      );

CREATE TABLE result_total_pvuv_min
(
    id        INT,
    name      STRING,
    money     decimal,
    dateone   timestamp,
    age       bigint,
    datethree timestamp,
    datesix   timestamp(6),
    datenigth timestamp(9),
    dtdate    date,
    dttime    time,
    primary key (id) NOT ENFORCED
) WITH (
      'connector' = 'emqx-x'
      ,'broker' = 'tcp://localhost:1883'
      ,'topic' = 'cx'
      ,'isCleanSession' = 'true'
      ,'qos' = '2'
      ,'username' = 'root'
      ,'password' = 'abc123'
      );


INSERT INTO result_total_pvuv_min
SELECT id
     , last_value(name)  as name
     , last_value(money) as money
     , max(dateone)      as dateone
     , sum(age)   as age
     , max(datethree)    as datethree
     , max(datesix)      as datesix
     , max(datenigth)    as datenigth
     , max(dtdate)       as dtdate
     , max(dttime)       as dttime
from source_ods_fact_user_ippv
group by id;
