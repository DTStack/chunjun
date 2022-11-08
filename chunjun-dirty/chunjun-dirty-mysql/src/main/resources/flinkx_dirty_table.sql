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

CREATE TABLE IF NOT EXISTS chunjun_dirty_data
(
    job_id        VARCHAR(32)                               NOT NULL COMMENT 'Flink Job Id',
    job_name      VARCHAR(255)                              NOT NULL COMMENT 'Flink Job Name',
    operator_name VARCHAR(255)                              NOT NULL COMMENT '出现异常数据的算子名，包含表名',
    dirty_data    TEXT                                      NOT NULL COMMENT '脏数据的异常数据',
    error_message TEXT COMMENT '脏数据中异常原因',
    field_name    VARCHAR(255) COMMENT '脏数据中异常字段名',
    create_time   TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '脏数据出现的时间点'
    )
    COMMENT '存储脏数据';

CREATE INDEX idx_job_id ON chunjun_dirty_data (job_id);
CREATE INDEX idx_operator_name ON chunjun_dirty_data (operator_name);
CREATE INDEX idx_create_time ON chunjun_dirty_data (create_time);
