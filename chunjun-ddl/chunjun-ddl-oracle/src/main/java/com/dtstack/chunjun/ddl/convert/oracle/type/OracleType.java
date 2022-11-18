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

package com.dtstack.chunjun.ddl.convert.oracle.type;

import com.dtstack.chunjun.cdc.ddl.ColumnType;

// https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-7B72E154-677A-4342-A1EA-C74C1EA928E6
public enum OracleType implements ColumnType {
    SMALLINT("SMALLINT"),
    INT("INT"),
    INTEGER("INTEGER"),
    VARCHAR("VARCHAR"),
    VARCHAR2("VARCHAR2"),
    NVARCHAR2("NVARCHAR2"),
    NUMBER("NUMBER"),
    DECIMAL("DECIMAL"),
    FLOAT("FLOAT"),
    // 不是数字Long  而是可变长度的字符数据，最大为 2 G
    LONG("LONG"),
    DATE("DATE"),
    BINARY_FLOAT("BINARY_FLOAT"),
    BINARY_DOUBLE("BINARY_DOUBLE"),
    TIMESTAMP("TIMESTAMP"),
    TIMESTAMP_WITH_TIME_ZONE("TIMESTAMP WITH TIME ZONE"),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE("TIMESTAMP WITH LOCAL TIME ZONE"),
    INTERVAL_YEAR_TO_MONTH("INTERVAL YEAR TO MONTH"),
    INTERVAL_DAY_TO_SECOND("INTERVAL DAY TO SECOND"),
    RAW("RAW"),
    LONG_RAW("LONG RAW"),
    ROWID("ROWID"),
    UROWID("NROWID"),
    CHAR("CHAR"),
    NCHAR("NCHAR"),
    CLOB("CLOB"),
    NCLOB("NCLOB"),
    BLOB("BLOB"),
    BFILE("BFILE"),
    JSON("JSON"),
    // 空间类型
    SDO_GEOMETRY("SDO_GEOMETRY"),
    SDO_TOPO_GEOMETRY("SDO_TOPO_GEOMETRY"),
    SDO_GEORASTER("SDO_GEORASTER"),

    XMLTYPE("XMLTYPE"),
    ;

    private final String sourceName;

    OracleType(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public String getSourceName() {
        return sourceName;
    }
}
