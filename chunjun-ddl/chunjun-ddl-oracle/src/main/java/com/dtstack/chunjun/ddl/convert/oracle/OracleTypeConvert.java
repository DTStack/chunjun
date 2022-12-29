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

package com.dtstack.chunjun.ddl.convert.oracle;

import com.dtstack.chunjun.cdc.ddl.ColumnType;
import com.dtstack.chunjun.cdc.ddl.ColumnTypeConvert;
import com.dtstack.chunjun.cdc.ddl.CommonColumnTypeEnum;
import com.dtstack.chunjun.cdc.ddl.CustomColumnType;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnTypeDesc;
import com.dtstack.chunjun.ddl.convert.oracle.type.OracleColumnTypeDescUtil;
import com.dtstack.chunjun.ddl.convert.oracle.type.OracleType;
import com.dtstack.chunjun.util.JsonUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

public class OracleTypeConvert implements ColumnTypeConvert {
    private static final HashMap<String, ColumnType> Oracle_TO_COMMON_TYPE = new HashMap<>(256);
    private static final HashMap<ColumnType, String> COMMON_TYPE_TO_ORACLE = new HashMap<>(256);
    private static final HashMap<ColumnType, ColumnTypeDesc> COMMON_TYPE_TO_COLUMN_TYPE_DESC =
            new HashMap<>(256);
    private static final List<String> ORACLE_COLUMN_TYPE = new ArrayList<>(128);

    private static final List<String> NOT_SUPPORT_TYPE = new ArrayList<String>(32);

    static {
        initNotSupportType();
        initOracleToCommonType();
        initCommonTypeToMysql();
        initAllColumnType();
    }

    public ColumnType conventDataSourceTypeToColumnType(String type) {
        if (NOT_SUPPORT_TYPE.contains(type.toUpperCase(Locale.ENGLISH))) {
            throw new RuntimeException(
                    "not support convert type: ["
                            + type
                            + " ], and these types are not supported: "
                            + JsonUtil.toJson(NOT_SUPPORT_TYPE));
        }

        if (Oracle_TO_COMMON_TYPE.containsKey(type.toUpperCase(Locale.ENGLISH))) {
            return Oracle_TO_COMMON_TYPE.get(type.toUpperCase(Locale.ENGLISH));
        }
        throw new RuntimeException("not support convert type: [" + type + "]");
    }

    @Override
    public String conventColumnTypeToDatSourceType(ColumnType type) {
        if (type instanceof CustomColumnType) {
            return type.getSourceName();
        }

        if (COMMON_TYPE_TO_ORACLE.containsKey(type)) {
            return COMMON_TYPE_TO_ORACLE.get(type);
        }
        throw new RuntimeException("not support convert type: [" + type.getSourceName() + " ]");
    }

    @Override
    public ColumnTypeDesc getColumnTypeDesc(ColumnType typeDesc) {
        return COMMON_TYPE_TO_COLUMN_TYPE_DESC.get(typeDesc);
    }

    @Override
    public List<String> getAllDataSourceColumnType() {
        return ORACLE_COLUMN_TYPE;
    }

    private static void initNotSupportType() {
        NOT_SUPPORT_TYPE.add(OracleType.INTERVAL_YEAR_TO_MONTH.getSourceName());
        NOT_SUPPORT_TYPE.add(OracleType.INTERVAL_DAY_TO_SECOND.getSourceName());
        NOT_SUPPORT_TYPE.add(OracleType.ROWID.getSourceName());
        NOT_SUPPORT_TYPE.add(OracleType.SDO_GEOMETRY.getSourceName());
        NOT_SUPPORT_TYPE.add(OracleType.SDO_TOPO_GEOMETRY.getSourceName());
        NOT_SUPPORT_TYPE.add(OracleType.SDO_GEORASTER.getSourceName());
    }

    private static void initOracleToCommonType() {
        Oracle_TO_COMMON_TYPE.put(
                OracleType.SMALLINT.getSourceName(), CommonColumnTypeEnum.SMALLINT);
        Oracle_TO_COMMON_TYPE.put(OracleType.INT.getSourceName(), CommonColumnTypeEnum.INTEGER);
        Oracle_TO_COMMON_TYPE.put(OracleType.INTEGER.getSourceName(), CommonColumnTypeEnum.INTEGER);
        Oracle_TO_COMMON_TYPE.put(OracleType.VARCHAR.getSourceName(), CommonColumnTypeEnum.VARCHAR);
        Oracle_TO_COMMON_TYPE.put(
                OracleType.VARCHAR2.getSourceName(), CommonColumnTypeEnum.VARCHAR);
        Oracle_TO_COMMON_TYPE.put(
                OracleType.NVARCHAR2.getSourceName(), CommonColumnTypeEnum.VARCHAR);
        Oracle_TO_COMMON_TYPE.put(OracleType.NUMBER.getSourceName(), CommonColumnTypeEnum.NUMERIC);
        Oracle_TO_COMMON_TYPE.put(OracleType.DECIMAL.getSourceName(), CommonColumnTypeEnum.DECIMAL);
        Oracle_TO_COMMON_TYPE.put(OracleType.FLOAT.getSourceName(), CommonColumnTypeEnum.FLOAT);

        Oracle_TO_COMMON_TYPE.put(OracleType.DATE.getSourceName(), CommonColumnTypeEnum.TIMESTAMP);
        Oracle_TO_COMMON_TYPE.put(
                OracleType.BINARY_FLOAT.getSourceName(), CommonColumnTypeEnum.FLOAT);
        Oracle_TO_COMMON_TYPE.put(
                OracleType.BINARY_DOUBLE.getSourceName(), CommonColumnTypeEnum.DOUBLE);
        Oracle_TO_COMMON_TYPE.put(
                OracleType.TIMESTAMP.getSourceName(), CommonColumnTypeEnum.TIMESTAMP);
        Oracle_TO_COMMON_TYPE.put(
                OracleType.TIMESTAMP_WITH_TIME_ZONE.getSourceName(),
                CommonColumnTypeEnum.TIMESTAMP_WITH_TIME_ZONE);
        Oracle_TO_COMMON_TYPE.put(
                OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getSourceName(),
                CommonColumnTypeEnum.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        Oracle_TO_COMMON_TYPE.put(OracleType.RAW.getSourceName(), CommonColumnTypeEnum.BLOB);
        Oracle_TO_COMMON_TYPE.put(OracleType.LONG_RAW.getSourceName(), CommonColumnTypeEnum.BLOB);
        Oracle_TO_COMMON_TYPE.put(OracleType.LONG.getSourceName(), CommonColumnTypeEnum.CLOB);
        Oracle_TO_COMMON_TYPE.put(OracleType.CHAR.getSourceName(), CommonColumnTypeEnum.CHAR);
        Oracle_TO_COMMON_TYPE.put(OracleType.NCHAR.getSourceName(), CommonColumnTypeEnum.CHAR);
        Oracle_TO_COMMON_TYPE.put(OracleType.CLOB.getSourceName(), CommonColumnTypeEnum.CLOB);
        Oracle_TO_COMMON_TYPE.put(OracleType.NCLOB.getSourceName(), CommonColumnTypeEnum.CLOB);
        Oracle_TO_COMMON_TYPE.put(OracleType.BLOB.getSourceName(), CommonColumnTypeEnum.BLOB);
        Oracle_TO_COMMON_TYPE.put(OracleType.BFILE.getSourceName(), CommonColumnTypeEnum.BLOB);
        Oracle_TO_COMMON_TYPE.put(OracleType.XMLTYPE.getSourceName(), CommonColumnTypeEnum.XML);
    }

    private static void initCommonTypeToMysql() {
        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.TINYINT, OracleType.NUMBER.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.TINYINT,
                OracleColumnTypeDescUtil.getNumber(CommonColumnTypeEnum.TINYINT.name()));

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.SMALLINT, OracleType.NUMBER.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.SMALLINT,
                OracleColumnTypeDescUtil.getNumber(CommonColumnTypeEnum.SMALLINT.name()));

        COMMON_TYPE_TO_ORACLE.put(
                CommonColumnTypeEnum.MEDIUMINT, OracleType.NUMBER.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.MEDIUMINT,
                OracleColumnTypeDescUtil.getNumber(CommonColumnTypeEnum.MEDIUMINT.name()));

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.INTEGER, OracleType.NUMBER.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.INTEGER,
                OracleColumnTypeDescUtil.getNumber(CommonColumnTypeEnum.INTEGER.name()));

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.BIGINT, OracleType.NUMBER.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.BIGINT,
                OracleColumnTypeDescUtil.getNumber(CommonColumnTypeEnum.BIGINT.name()));

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.FLOAT, OracleType.FLOAT.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.FLOAT, OracleColumnTypeDescUtil.FLOAT);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.DOUBLE, OracleType.FLOAT.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.DOUBLE, OracleColumnTypeDescUtil.FLOAT);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.DECIMAL, OracleType.NUMBER.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.DECIMAL, OracleColumnTypeDescUtil.NUMBER);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.NUMERIC, OracleType.NUMBER.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.NUMERIC, OracleColumnTypeDescUtil.NUMBER);

        COMMON_TYPE_TO_ORACLE.put(
                CommonColumnTypeEnum.VARCHAR, OracleType.VARCHAR2.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.VARCHAR, OracleColumnTypeDescUtil.VARCHAR2);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.CHAR, OracleType.CHAR.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.CHAR, OracleColumnTypeDescUtil.CHAR);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.DATE, OracleType.DATE.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.DATE, OracleColumnTypeDescUtil.DATE);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.TIME, OracleType.VARCHAR.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.TIME, OracleColumnTypeDescUtil.VARCHAR);

        COMMON_TYPE_TO_ORACLE.put(
                CommonColumnTypeEnum.DATETIME, OracleType.TIMESTAMP.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.DATETIME, OracleColumnTypeDescUtil.TIMESTAMP);

        COMMON_TYPE_TO_ORACLE.put(
                CommonColumnTypeEnum.TIMESTAMP, OracleType.TIMESTAMP.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.TIMESTAMP, OracleColumnTypeDescUtil.TIMESTAMP);

        COMMON_TYPE_TO_ORACLE.put(
                CommonColumnTypeEnum.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                OracleColumnTypeDescUtil.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

        COMMON_TYPE_TO_ORACLE.put(
                CommonColumnTypeEnum.TIMESTAMP_WITH_TIME_ZONE,
                OracleType.TIMESTAMP_WITH_TIME_ZONE.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.TIMESTAMP_WITH_TIME_ZONE,
                OracleColumnTypeDescUtil.TIMESTAMP_WITH_TIME_ZONE);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.BIT, OracleType.BLOB.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.BIT, OracleColumnTypeDescUtil.BLOB);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.BOOLEAN, OracleType.NUMBER.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.BOOLEAN, OracleColumnTypeDescUtil.NUMBER);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.BINARY, OracleType.BLOB.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.BINARY, OracleColumnTypeDescUtil.BLOB);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.BLOB, OracleType.BLOB.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.BLOB, OracleColumnTypeDescUtil.BLOB);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.CLOB, OracleType.CLOB.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.CLOB, OracleColumnTypeDescUtil.CLOB);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.JSON, OracleType.CLOB.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.JSON, OracleColumnTypeDescUtil.CLOB);

        COMMON_TYPE_TO_ORACLE.put(CommonColumnTypeEnum.XML, OracleType.XMLTYPE.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.XML, OracleColumnTypeDescUtil.XMLTYPE);
    }

    private static void initAllColumnType() {
        Arrays.stream(OracleType.values())
                .forEach(
                        i -> {
                            ORACLE_COLUMN_TYPE.add(i.name());
                        });
    }
}
