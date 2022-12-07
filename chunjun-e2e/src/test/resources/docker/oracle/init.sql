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

create table debezium.ALL_TYPE1_SOURCE
(
    VARCHAR2_TYPE                       VARCHAR2(255),
    NVARCHAR2_TYPE                      NVARCHAR2(255),
    NUMBER_TYPE                         NUMBER(12, 12),
    FLOAT_TYPE                          FLOAT(12),
    LONG_TYPE                           LONG,
    DATE_TYPE                           DATE,
    BINARY_FLOAT_TYPE                   BINARY_FLOAT,
    BINARY_DOUBLE_TYPE                  BINARY_DOUBLE,
    TIMESTAMP_TYPE                      TIMESTAMP(6),
    TIMESTAMP9_TYPE                     TIMESTAMP(9),
    TIMESTAMP_TIME_ZONE_TYPE       TIMESTAMP(9) WITH TIME ZONE,
    TIMESTAMP_LOCAL_TIME_ZONE_TYPE TIMESTAMP(9) WITH LOCAL TIME ZONE,
    INTERVAL_YEAR_TO_MONTH_TYPE         INTERVAL YEAR(2) TO MONTH,
    INTERVAL_DAY_TO_SECOND_TYPE         INTERVAL DAY(2) TO SECOND(6),
    RAW_TYPE                            RAW(255),
    INTERVAL_DAY6_TO_SECOND_TYPE        INTERVAL DAY(6) TO SECOND(6),
    INTERVAL_YEAR6_TO_MONTH_TYPE        INTERVAL YEAR(6) TO MONTH,
    UROWID_TYPE                         UROWID,
    CHAR_TYPE                           CHAR(255),
    NCHAR_TYPE                          NCHAR(255),
    CLOB_TYPE                           CLOB,
    NCLOB_TYPE                          NCLOB,
    BFILE_TYPE                          BFILE,
    BLOB_TYPE                           BLOB
);

create table debezium.ALL_TYPE1_SINK
(
    VARCHAR2_TYPE                       VARCHAR2(255),
    NVARCHAR2_TYPE                      NVARCHAR2(255),
    NUMBER_TYPE                         NUMBER(12, 12),
    FLOAT_TYPE                          FLOAT(12),
    LONG_TYPE                           LONG,
    DATE_TYPE                           DATE,
    BINARY_FLOAT_TYPE                   BINARY_FLOAT,
    BINARY_DOUBLE_TYPE                  BINARY_DOUBLE,
    TIMESTAMP_TYPE                      TIMESTAMP(6),
    TIMESTAMP9_TYPE                     TIMESTAMP(9),
    TIMESTAMP_TIME_ZONE_TYPE       TIMESTAMP(9) WITH TIME ZONE,
    TIMESTAMP_LOCAL_TIME_ZONE_TYPE TIMESTAMP(9) WITH LOCAL TIME ZONE,
    INTERVAL_YEAR_TO_MONTH_TYPE         INTERVAL YEAR(2) TO MONTH,
    INTERVAL_DAY_TO_SECOND_TYPE         INTERVAL DAY(2) TO SECOND(6),
    RAW_TYPE                            RAW(255),
    INTERVAL_DAY6_TO_SECOND_TYPE        INTERVAL DAY(6) TO SECOND(6),
    INTERVAL_YEAR6_TO_MONTH_TYPE        INTERVAL YEAR(6) TO MONTH,
    UROWID_TYPE                         UROWID,
    CHAR_TYPE                           CHAR(255),
    NCHAR_TYPE                          NCHAR(255),
    CLOB_TYPE                           CLOB,
    NCLOB_TYPE                          NCLOB,
    BFILE_TYPE                          BFILE,
    BLOB_TYPE                           BLOB
);

create table debezium.ALL_TYPE2_SOURCE
(
    VARCHAR2_TYPE                       VARCHAR2(255),
    NVARCHAR2_TYPE                      NVARCHAR2(255),
    NUMBER_TYPE                         NUMBER(12, 12),
    FLOAT_TYPE                          FLOAT(12),
    LONG_RAW_TYPE                       LONG RAW,
    DATE_TYPE                           DATE,
    BINARY_FLOAT_TYPE                   BINARY_FLOAT,
    BINARY_DOUBLE_TYPE                  BINARY_DOUBLE,
    TIMESTAMP_TYPE                      TIMESTAMP(6),
    TIMESTAMP9_TYPE                     TIMESTAMP(9),
    TIMESTAMP_TIME_ZONE_TYPE       TIMESTAMP(9) WITH TIME ZONE,
    TIMESTAMP_LOCAL_TIME_ZONE_TYPE TIMESTAMP(9) WITH LOCAL TIME ZONE,
    INTERVAL_YEAR_TO_MONTH_TYPE         INTERVAL YEAR(2) TO MONTH,
    INTERVAL_DAY_TO_SECOND_TYPE         INTERVAL DAY(2) TO SECOND(6),
    RAW_TYPE                            RAW(255),
    INTERVAL_DAY6_TO_SECOND_TYPE        INTERVAL DAY(6) TO SECOND(6),
    INTERVAL_YEAR6_TO_MONTH_TYPE        INTERVAL YEAR(6) TO MONTH,
    UROWID_TYPE                         UROWID,
    CHAR_TYPE                           CHAR(255),
    NCHAR_TYPE                          NCHAR(255),
    CLOB_TYPE                           CLOB,
    NCLOB_TYPE                          NCLOB,
    BFILE_TYPE                          BFILE,
    BLOB_TYPE                           BLOB
);

create table debezium.ALL_TYPE2_SINK
(
    VARCHAR2_TYPE                       VARCHAR2(255),
    NVARCHAR2_TYPE                      NVARCHAR2(255),
    NUMBER_TYPE                         NUMBER(12, 12),
    FLOAT_TYPE                          FLOAT(12),
    LONG_RAW_TYPE                       LONG RAW,
    DATE_TYPE                           DATE,
    BINARY_FLOAT_TYPE                   BINARY_FLOAT,
    BINARY_DOUBLE_TYPE                  BINARY_DOUBLE,
    TIMESTAMP_TYPE                      TIMESTAMP(6),
    TIMESTAMP9_TYPE                     TIMESTAMP(9),
    TIMESTAMP_TIME_ZONE_TYPE       TIMESTAMP(9) WITH TIME ZONE,
    TIMESTAMP_LOCAL_TIME_ZONE_TYPE TIMESTAMP(9) WITH LOCAL TIME ZONE,
    INTERVAL_YEAR_TO_MONTH_TYPE         INTERVAL YEAR(2) TO MONTH,
    INTERVAL_DAY_TO_SECOND_TYPE         INTERVAL DAY(2) TO SECOND(6),
    RAW_TYPE                            RAW(255),
    INTERVAL_DAY6_TO_SECOND_TYPE        INTERVAL DAY(6) TO SECOND(6),
    INTERVAL_YEAR6_TO_MONTH_TYPE        INTERVAL YEAR(6) TO MONTH,
    UROWID_TYPE                         UROWID,
    CHAR_TYPE                           CHAR(255),
    NCHAR_TYPE                          NCHAR(255),
    CLOB_TYPE                           CLOB,
    NCLOB_TYPE                          NCLOB,
    BFILE_TYPE                          BFILE,
    BLOB_TYPE                           BLOB
);

create table debezium.TEST_SOURCE
(
    INT_VAL       NUMBER,
    DOUBLE_VAL    FLOAT,
    DATE_VAL      DATE,
    TIMESTAMP_VAL TIMESTAMP(6),
    VAR_VAL       VARCHAR2(255),
    NAME          VARCHAR2(255),
    MESSAGE       VARCHAR2(255)
);

create table debezium.TEST_SINK
(
    INT_VAL       NUMBER,
    DOUBLE_VAL    FLOAT,
    DATE_VAL      DATE,
    TIMESTAMP_VAL TIMESTAMP(6),
    VAR_VAL       VARCHAR2(255),
    NAME          VARCHAR2(255),
    MESSAGE       VARCHAR2(255)
);

INSERT INTO debezium.TEST_SOURCE (INT_VAL, DOUBLE_VAL, DATE_VAL, TIMESTAMP_VAL, VAR_VAL, NAME, MESSAGE)
VALUES (1, 4086.104923538155, TO_DATE('2095-02-04 15:59:22', 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('2022-08-03 14:11:12.651000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'FdTY', 'Abc', 'Hello');
INSERT INTO debezium.TEST_SOURCE (INT_VAL, DOUBLE_VAL, DATE_VAL, TIMESTAMP_VAL, VAR_VAL, NAME, MESSAGE)
VALUES (2, 9401.154078754176, TO_DATE('1984-10-27 23:04:04', 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('2022-08-03 14:11:12.665000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'kPDM', 'Abc', 'Hello');
INSERT INTO debezium.TEST_SOURCE (INT_VAL, DOUBLE_VAL, DATE_VAL, TIMESTAMP_VAL, VAR_VAL, NAME, MESSAGE)
VALUES (3, 3654.8354065891676, TO_DATE('2082-11-01 05:25:45', 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('2022-08-03 14:11:12.665000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'fwhi7A', 'Abc', 'Hello');
INSERT INTO debezium.TEST_SOURCE (INT_VAL, DOUBLE_VAL, DATE_VAL, TIMESTAMP_VAL, VAR_VAL, NAME, MESSAGE)
VALUES (4, 1700.5049489644764, TO_DATE('2060-02-01 03:18:48', 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('2022-08-03 14:11:12.666000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'Vam', 'Abc', 'Hello');
INSERT INTO debezium.TEST_SOURCE (INT_VAL, DOUBLE_VAL, DATE_VAL, TIMESTAMP_VAL, VAR_VAL, NAME, MESSAGE)
VALUES (5, 7213.916066384409, TO_DATE('2027-11-14 21:55:03', 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('2022-08-03 14:11:12.666000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'X2QZAo', 'Abc', 'Hello');
INSERT INTO debezium.TEST_SOURCE (INT_VAL, DOUBLE_VAL, DATE_VAL, TIMESTAMP_VAL, VAR_VAL, NAME, MESSAGE)
VALUES (7, 7494.472210715716, TO_DATE('2096-02-08 06:28:10', 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('2022-08-03 14:11:12.668000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'zW6QXgrz', 'Abc', 'Hello');
INSERT INTO debezium.TEST_SOURCE (INT_VAL, DOUBLE_VAL, DATE_VAL, TIMESTAMP_VAL, VAR_VAL, NAME, MESSAGE)
VALUES (8, 4082.4893142314077, TO_DATE('2064-02-09 08:22:15', 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('2022-08-03 14:11:12.668000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'bLLICJ4', 'Abc', 'Hello');
INSERT INTO debezium.TEST_SOURCE (INT_VAL, DOUBLE_VAL, DATE_VAL, TIMESTAMP_VAL, VAR_VAL, NAME, MESSAGE)
VALUES (9, 2248.440916449925, TO_DATE('2089-10-14 08:56:57', 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('2022-08-03 14:11:12.669000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'OYB4jD8s', 'Abc', 'Hello');
INSERT INTO debezium.TEST_SOURCE (INT_VAL, DOUBLE_VAL, DATE_VAL, TIMESTAMP_VAL, VAR_VAL, NAME, MESSAGE)
VALUES (10, 1363.0987942903073, TO_DATE('1991-11-11 00:46:38', 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('2022-08-03 14:11:12.670000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'NqDOi', 'Abc', 'Hello');
INSERT INTO debezium.TEST_SOURCE (INT_VAL, DOUBLE_VAL, DATE_VAL, TIMESTAMP_VAL, VAR_VAL, NAME, MESSAGE)
VALUES (11, 9036.620205198631, TO_DATE('2040-03-20 13:40:13', 'YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('2022-08-03 14:11:12.671000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 'l4bezLJ', 'Abc', 'Hello');

CREATE TABLE debezium.ORACLE_TEST
(
    TEST_INT NUMBER NOT NULL,
    TEST_VARCHAR NVARCHAR2(200),
    TEST_CHAR NVARCHAR2(200)
);

CREATE TABLE debezium.ORACLE_TEST_LOGMINER
(
    TEST_INT NUMBER NOT NULL,
    TEST_VARCHAR NVARCHAR2(200),
    TEST_CHAR NVARCHAR2(200)
);

INSERT INTO debezium.ORACLE_TEST(TEST_INT,TEST_VARCHAR,TEST_CHAR) values(1000010,'大海','中国人');
INSERT INTO debezium.ORACLE_TEST(TEST_INT,TEST_VARCHAR,TEST_CHAR) values(1000011,'大海','中国人');
INSERT INTO debezium.ORACLE_TEST(TEST_INT,TEST_VARCHAR,TEST_CHAR) values(1000012,'大海','中国人');
INSERT INTO debezium.ORACLE_TEST(TEST_INT,TEST_VARCHAR,TEST_CHAR) values(1000013,'大海','中国人');
INSERT INTO debezium.ORACLE_TEST(TEST_INT,TEST_VARCHAR,TEST_CHAR) values(1000014,'大海','中国人');
