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

import com.dtstack.chunjun.cdc.ddl.definition.ColumnTypeDesc;

public class OracleColumnTypeDescUtil {
    public static ColumnTypeDesc VARCHAR =
            new ColumnTypeDesc(OracleType.VARCHAR, 255, null, 2000, null, 1, null, true);
    public static ColumnTypeDesc VARCHAR2 =
            new ColumnTypeDesc(OracleType.VARCHAR2, 255, null, null, null, 1, null, true);
    public static ColumnTypeDesc NVARCHAR2 =
            new ColumnTypeDesc(OracleType.NVARCHAR2, 255, null, 2000, null, 1, null, true);
    public static ColumnTypeDesc NUMBER =
            new ColumnTypeDesc(OracleType.NUMBER, 38, 8, 38, 127, 1, -84, true);
    public static ColumnTypeDesc FLOAT = new ColumnTypeDesc(OracleType.FLOAT, true);
    public static ColumnTypeDesc LONG = new ColumnTypeDesc(OracleType.LONG, false);
    public static ColumnTypeDesc DATE = new ColumnTypeDesc(OracleType.DATE, false);
    public static ColumnTypeDesc BINARY_FLOAT = new ColumnTypeDesc(OracleType.BINARY_FLOAT, false);
    public static ColumnTypeDesc BINARY_DOUBLE =
            new ColumnTypeDesc(OracleType.BINARY_DOUBLE, false);
    public static ColumnTypeDesc TIMESTAMP = new ColumnTypeDesc(OracleType.TIMESTAMP, true);
    public static ColumnTypeDesc TIMESTAMP_WITH_TIME_ZONE =
            new ColumnTypeDesc(OracleType.TIMESTAMP_WITH_TIME_ZONE, true);
    public static ColumnTypeDesc TIMESTAMP_WITH_LOCAL_TIME_ZONE =
            new ColumnTypeDesc(OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE, true);
    public static ColumnTypeDesc INTERVAL_YEAR_TO_MONTH =
            new ColumnTypeDesc(OracleType.INTERVAL_YEAR_TO_MONTH, true);
    public static ColumnTypeDesc INTERVAL_DAY_TO_SECOND =
            new ColumnTypeDesc(OracleType.INTERVAL_DAY_TO_SECOND, true);
    public static ColumnTypeDesc RAW =
            new ColumnTypeDesc(OracleType.RAW, 512, null, 2000, null, 1, null, true);
    public static ColumnTypeDesc LONG_RAW = new ColumnTypeDesc(OracleType.LONG_RAW, false);
    public static ColumnTypeDesc ROWID = new ColumnTypeDesc(OracleType.ROWID, false);
    public static ColumnTypeDesc CHAR = new ColumnTypeDesc(OracleType.CHAR, true);
    public static ColumnTypeDesc NCHAR = new ColumnTypeDesc(OracleType.NCHAR, true);
    public static ColumnTypeDesc CLOB = new ColumnTypeDesc(OracleType.CLOB, false);
    public static ColumnTypeDesc NCLOB = new ColumnTypeDesc(OracleType.NCLOB, false);
    public static ColumnTypeDesc BLOB = new ColumnTypeDesc(OracleType.BLOB, false);
    public static ColumnTypeDesc BFILE = new ColumnTypeDesc(OracleType.BFILE, false);
    public static ColumnTypeDesc SDO_GEOMETRY = new ColumnTypeDesc(OracleType.SDO_GEOMETRY, false);
    public static ColumnTypeDesc SDO_TOPO_GEOMETRY =
            new ColumnTypeDesc(OracleType.SDO_TOPO_GEOMETRY, false);
    public static ColumnTypeDesc SDO_GEORASTER =
            new ColumnTypeDesc(OracleType.SDO_GEORASTER, false);
    public static ColumnTypeDesc XMLTYPE = new ColumnTypeDesc(OracleType.XMLTYPE, false);

    public static ColumnTypeDesc getNumber(String type) {

        if ("INTEGER".equalsIgnoreCase(type)
                || "SMALLINT".equalsIgnoreCase(type)
                || "BIGINT".equalsIgnoreCase(type)
                || "TINYINT".equalsIgnoreCase(type)) {
            return new ColumnTypeDesc(OracleType.NUMBER, 38, 0, 38, 0, 38, 0, true);
        }
        return NUMBER;
    }
}
