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
      'connector' = 'emqx-x'
      ,'broker' = 'tcp://localhost:1883'
      ,'topic' = 'cx'
      ,'isCleanSession' = 'true'
      ,'qos' = '2'
      ,'username' = 'username'
      ,'password' = 'password'
      ,'format' = 'json'
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
    dttime    time

) WITH (
      -- 'connector' = 'stream-x'

      'connector' = 'emqx-x'
      ,'broker' = 'tcp://localhost:1883'
      ,'topic' = 'da'
      ,'isCleanSession' = 'true'
      ,'qos' = '2'
      ,'username' = 'username'
      ,'password' = 'password'
      );


INSERT INTO result_total_pvuv_min
SELECT *
from source_ods_fact_user_ippv;
