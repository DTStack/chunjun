-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.


CREATE TABLE MyResult
(
    id   INT,
    name INT
) WITH (
      'print' = 'true',
      'connector' = 'stream-x'
      );
CREATE TABLE mywb
(
    id   INT,
    name INT,
    age  INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'password' = 'admin123',
      'connector' = 'mysql-x',
      'lookup.cache-type' = 'LRU',
      'lookup.parallelism' = '1',
      'vertx.worker-pool-size' = '5',
      'lookup.cache.ttl' = '60000',
      'lookup.cache.max-rows' = '10000',
      'table-name' = 'test_five',
      'url' = 'jdbc:mysql://k3:3306/tiezhu?useSSL=false',
      'username' = 'root'
      );
insert
into MyResult
select yb.id,
       wb.name
from MyTable yb
         left join
     mywb FOR SYSTEM_TIME AS OF yb.proc_time AS wb
     on yb.id = wb.id;
