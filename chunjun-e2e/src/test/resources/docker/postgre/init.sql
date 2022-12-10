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

DROP SCHEMA IF EXISTS inventory CASCADE;
CREATE SCHEMA inventory;
SET search_path TO inventory;

-- Create and populate our products using a single insert with many rows
CREATE TABLE inventory.products (
                          id SERIAL NOT NULL PRIMARY KEY,
                          name VARCHAR(255) NOT NULL DEFAULT 'flink',
                          description VARCHAR(512),
                          weight FLOAT
);
ALTER SEQUENCE products_id_seq RESTART WITH 101;
ALTER TABLE products REPLICA IDENTITY FULL;

CREATE TABLE inventory.products_sink (
                          id SERIAL NOT NULL PRIMARY KEY,
                          name VARCHAR(255) NOT NULL DEFAULT 'flink',
                          description VARCHAR(512),
                          weight FLOAT
);

INSERT INTO inventory.products
VALUES (default,'scooter','Small 2-wheel scooter',3.14),
       (default,'car battery','12V car battery',8.1),
       (default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8),
       (default,'hammer','12oz carpenter''s hammer',0.75),
       (default,'hammer','14oz carpenter''s hammer',0.875),
       (default,'hammer','16oz carpenter''s hammer',1.0),
       (default,'rocks','box of assorted rocks',5.3),
       (default,'jacket','water resistent black wind breaker',0.1),
       (default,'spare tire','24 inch spare tire',22.2);
