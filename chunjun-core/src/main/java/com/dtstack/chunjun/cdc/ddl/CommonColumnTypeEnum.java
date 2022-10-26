/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.cdc.ddl;

public enum CommonColumnTypeEnum implements ColumnType {
    TINYINT,
    SMALLINT,
    MEDIUMINT,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    NUMERIC,
    VARCHAR,
    CHAR,
    BIT,
    BOOLEAN,
    DATE,
    TIME,
    DATETIME,
    TIMESTAMP,
    TIMESTAMP_WITH_TIME_ZONE,
    TIMESTAMP_WITH_LOCAL_TIME_ZONE,
    // 小数据量二进制数据
    BINARY,
    // 大数据量二进制数据
    BLOB,
    // 大数据量字符串
    CLOB,
    JSON,
    XML,
    ;

    @Override
    public String getSourceName() {
        return this.name();
    }
}
