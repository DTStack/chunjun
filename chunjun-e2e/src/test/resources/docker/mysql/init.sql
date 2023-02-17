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

create database `chunjun`;

set character_set_client = utf8;

use `chunjun`;

drop table if exists test;

create table if not exists test
(
    id             int(11)      not null AUTO_INCREMENT PRIMARY KEY,
    name           varchar(100) null,
    idcard         varchar(100) null,
    birthday       date         null,
    mobile         varchar(100) null,
    email          varchar(100) null,
    gender         tinyint      null,
    create_time    timestamp    not null default current_timestamp on update current_timestamp,
    type_smallint  smallint(6)           default null,
    type_mediumint mediumint(9)          DEFAULT NULL,
    type_bigint    bigint(20)            DEFAULT NULL,
    type_decimal   decimal(10, 2),
    type_float     float(10, 2)          DEFAULT NULL,
    type_double    double(10, 2)         DEFAULT NULL,
    type_text      text
    );

INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (1, 'zhang1', '1', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00, 1,
        1, '1');
INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (2, 'zhang2', '2', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00, 1,
        1, '1');
INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (3, 'zhang3', '3', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00, 1,
        1, '1');
INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (4, 'zhang4', '4', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00, 1,
        1, '1');
INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (5, 'zhang5', '5', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00, 1,
        1, '1');
INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (6, 'zhang6', '6', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00, 1,
        1, '1');
INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (7, 'zhang7', '7', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00, 1,
        1, '1');
INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (8, 'zhang8', '8', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00, 1,
        1, '1');
INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (9, 'zhang9', '9', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00, 1,
        1, '1');
INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (10, 'zhang10', '10', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00,
        1,
        1, '1');
INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (11, 'zhang11', '11', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00,
        1,
        1, '1');
INSERT INTO chunjun.test (id, name, idcard, birthday, mobile, email, gender, create_time, type_smallint, type_mediumint,
                          type_bigint, type_decimal, type_float, type_double, type_text)
VALUES (12, 'zhang12', '12', '2022-08-12', '13022539328', 'zhang3@chunjun', 1, '2022-08-12 11:20:15', 1, 1, 1, 1.00,
        1,
        1, '1');

create table if not exists test_sink
(
    id             int(11)      not null AUTO_INCREMENT PRIMARY KEY,
    name           varchar(100) null,
    idcard         varchar(100) null,
    birthday       date         null,
    mobile         varchar(100) null,
    email          varchar(100) null,
    gender         tinyint      null,
    create_time    timestamp    not null default current_timestamp on update current_timestamp,
    type_smallint  smallint(6)           default null,
    type_mediumint mediumint(9)          DEFAULT NULL,
    type_bigint    bigint(20)            DEFAULT NULL,
    type_decimal   decimal(10, 2),
    type_float     float(10, 2)          DEFAULT NULL,
    type_double    double(10, 2)         DEFAULT NULL,
    type_text      text
    );
