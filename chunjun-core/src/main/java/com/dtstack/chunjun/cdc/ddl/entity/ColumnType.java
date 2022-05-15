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

package com.dtstack.chunjun.cdc.ddl.entity;

public enum ColumnType {
    BOOLEAN(),
    TINYINT(),
    SMALLINT(),
    INTEGER(),
    BIGINT(),
    DECIMAL(),
    FLOAT(),
    REAL(),
    DOUBLE(),
    DATE(),
    TIME(),
    TIME_WITH_LOCAL_TIME_ZONE(),
    TIMESTAMP(),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
    INTERVAL_YEAR(),
    INTERVAL_YEAR_MONTH(),
    INTERVAL_MONTH(),
    INTERVAL_DAY(),
    INTERVAL_DAY_HOUR(),
    INTERVAL_DAY_MINUTE(),
    INTERVAL_DAY_SECOND(),
    INTERVAL_HOUR(),
    INTERVAL_HOUR_MINUTE(),
    INTERVAL_HOUR_SECOND(),
    INTERVAL_MINUTE(),
    INTERVAL_MINUTE_SECOND(),
    INTERVAL_SECOND(),
    CHAR(),
    VARCHAR(),
    BINARY(),
    VARBINARY(),
    NULL(),
    ANY(),
    SYMBOL(),
    MULTISET(),
    ARRAY(),
    MAP(),
    DISTINCT(),
    STRUCTURED(),
    ROW(),
    OTHER(),
    CURSOR(),
    COLUMN_LIST(),
    DYNAMIC_STAR(),
    GEOMETRY(),
    SARG();

    ColumnType() {}
}
