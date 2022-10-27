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

-- auto-generated definition
create table ddl_change
(
    database_name  varchar(200)                              not null comment 'DDL操作对应的database_name',
    table_name     varchar(200)                              not null comment 'DDL操作对应的table_name',
    operation_type varchar(200)                              not null comment 'DDL操作对应的类型，如：alter、create等',
    lsn            varchar(100)                              not null comment 'DDL操作在binlog日志中的位点',
    content        text null comment 'DDL操作对应的SQL语句',
    update_time    timestamp(6) default CURRENT_TIMESTAMP(6) not null on update CURRENT_TIMESTAMP (6) comment 'DDL操作时间',
    status         smallint     default 0                    not null comment 'DDL操作对应的状态，0表示未执行，2表示已执行',
    constraint ddl_change_pk
        unique (database_name, table_name, lsn)
) comment '存储DDL操作';

create index ddl_change_lsn_update_time_operation_type_index
    on ddl_change (lsn, update_time, operation_type);

