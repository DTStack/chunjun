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

package com.dtstack.chunjun.ddl.convent.mysql;

import com.dtstack.chunjun.cdc.ddl.ColumnType;
import com.dtstack.chunjun.cdc.ddl.ColumnTypeConvert;
import com.dtstack.chunjun.cdc.ddl.CommonColumnTypeEnum;
import com.dtstack.chunjun.cdc.ddl.CustomColumnType;
import com.dtstack.chunjun.cdc.ddl.definition.ColumnTypeDesc;
import com.dtstack.chunjun.ddl.convent.mysql.type.MysqlColumnTypeDescUtil;
import com.dtstack.chunjun.ddl.convent.mysql.type.MysqlType;
import com.dtstack.chunjun.util.JsonUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import static com.dtstack.chunjun.ddl.convent.mysql.type.MysqlType.GEOMCOLLECTION;
import static com.dtstack.chunjun.ddl.convent.mysql.type.MysqlType.GEOMETRY;
import static com.dtstack.chunjun.ddl.convent.mysql.type.MysqlType.LINESTRING;
import static com.dtstack.chunjun.ddl.convent.mysql.type.MysqlType.MULTILINESTRING;
import static com.dtstack.chunjun.ddl.convent.mysql.type.MysqlType.MULTIPOINT;
import static com.dtstack.chunjun.ddl.convent.mysql.type.MysqlType.MULTIPOLYGON;
import static com.dtstack.chunjun.ddl.convent.mysql.type.MysqlType.POLYGON;

public class MysqlTypeConvert implements ColumnTypeConvert {
    private static final HashMap<String, ColumnType> MYSQL_TO_COMMON_TYPE = new HashMap<>(256);
    private static final HashMap<ColumnType, String> COMMON_TYPE_TO_MYSQL = new HashMap<>(256);
    private static final List<String> MYSQL_COLUMN_TYPE = new ArrayList<>(128);
    private static final HashMap<ColumnType, ColumnTypeDesc> COMMON_TYPE_TO_COLUMN_TYPE_DESC =
            new HashMap<>(256);

    private static final List<String> NOT_SUPPORT_TYPE = new ArrayList<String>(32);

    static {
        initNotSupportType();
        initMysqlToCommonType();
        initCommonTypeToMysql();
        initAllColumnType();
    }

    @Override
    public ColumnType conventDataSourceTypeToColumnType(String type) {
        if (NOT_SUPPORT_TYPE.contains(type.toUpperCase(Locale.ENGLISH))) {
            throw new RuntimeException(
                    "not support convent type: ["
                            + type
                            + " ], and these types are not supported: "
                            + JsonUtil.toJson(NOT_SUPPORT_TYPE));
        }

        if (MYSQL_TO_COMMON_TYPE.containsKey(type.toUpperCase(Locale.ENGLISH))) {
            return MYSQL_TO_COMMON_TYPE.get(type.toUpperCase(Locale.ENGLISH));
        }
        throw new RuntimeException("not support convent type: [" + type + "]");
    }

    public String conventColumnTypeToDatSourceType(ColumnType type) {
        if (type instanceof CustomColumnType) {
            return type.getSourceName();
        }

        if (COMMON_TYPE_TO_MYSQL.containsKey(type)) {
            return COMMON_TYPE_TO_MYSQL.get(type);
        }
        throw new RuntimeException("not support convent type: [" + type.getSourceName() + " ]");
    }

    @Override
    public ColumnTypeDesc getColumnTypeDesc(ColumnType type) {
        return COMMON_TYPE_TO_COLUMN_TYPE_DESC.get(type);
    }

    @Override
    public List<String> getAllDataSourceColumnType() {
        return MYSQL_COLUMN_TYPE;
    }

    private static void initNotSupportType() {
        NOT_SUPPORT_TYPE.add(GEOMETRY.getSourceName());
        NOT_SUPPORT_TYPE.add(LINESTRING.getSourceName());
        NOT_SUPPORT_TYPE.add(POLYGON.getSourceName());
        NOT_SUPPORT_TYPE.add(MULTIPOINT.getSourceName());
        NOT_SUPPORT_TYPE.add(MULTILINESTRING.getSourceName());
        NOT_SUPPORT_TYPE.add(MULTIPOLYGON.getSourceName());
        NOT_SUPPORT_TYPE.add(GEOMCOLLECTION.getSourceName());
    }

    private static void initMysqlToCommonType() {
        MYSQL_TO_COMMON_TYPE.put(MysqlType.SERIAL.getSourceName(), CommonColumnTypeEnum.BIGINT);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.TINYINT.getSourceName(), CommonColumnTypeEnum.TINYINT);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.SMALLINT.getSourceName(), CommonColumnTypeEnum.SMALLINT);
        MYSQL_TO_COMMON_TYPE.put(
                MysqlType.MEDIUMINT.getSourceName(), CommonColumnTypeEnum.MEDIUMINT);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.INTEGER.getSourceName(), CommonColumnTypeEnum.INTEGER);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.INT.getSourceName(), CommonColumnTypeEnum.INTEGER);

        MYSQL_TO_COMMON_TYPE.put(MysqlType.BIGINT.getSourceName(), CommonColumnTypeEnum.BIGINT);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.VARCHAR.getSourceName(), CommonColumnTypeEnum.VARCHAR);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.CHAR.getSourceName(), CommonColumnTypeEnum.CHAR);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.FLOAT.getSourceName(), CommonColumnTypeEnum.FLOAT);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.REAL.getSourceName(), CommonColumnTypeEnum.FLOAT);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.DOUBLE.getSourceName(), CommonColumnTypeEnum.DOUBLE);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.DECIMAL.getSourceName(), CommonColumnTypeEnum.DECIMAL);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.NUMERIC.getSourceName(), CommonColumnTypeEnum.NUMERIC);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.BIT.getSourceName(), CommonColumnTypeEnum.BIT);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.BOOLEAN.getSourceName(), CommonColumnTypeEnum.BOOLEAN);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.DATE.getSourceName(), CommonColumnTypeEnum.DATE);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.TIME.getSourceName(), CommonColumnTypeEnum.TIME);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.DATETIME.getSourceName(), CommonColumnTypeEnum.DATETIME);
        MYSQL_TO_COMMON_TYPE.put(
                MysqlType.TIMESTAMP.getSourceName(), CommonColumnTypeEnum.TIMESTAMP);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.BINARY.getSourceName(), CommonColumnTypeEnum.BINARY);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.VARBINARY.getSourceName(), CommonColumnTypeEnum.BINARY);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.TEXT.getSourceName(), CommonColumnTypeEnum.CLOB);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.TINYTEXT.getSourceName(), CommonColumnTypeEnum.CLOB);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.MEDIUMTEXT.getSourceName(), CommonColumnTypeEnum.CLOB);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.LONG.getSourceName(), CommonColumnTypeEnum.CLOB);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.LONGTEXT.getSourceName(), CommonColumnTypeEnum.CLOB);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.TINYBLOB.getSourceName(), CommonColumnTypeEnum.BLOB);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.MEDIUMBLOB.getSourceName(), CommonColumnTypeEnum.BLOB);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.BLOB.getSourceName(), CommonColumnTypeEnum.BLOB);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.LONGBLOB.getSourceName(), CommonColumnTypeEnum.BLOB);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.YEAR.getSourceName(), CommonColumnTypeEnum.VARCHAR);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.ENUM.getSourceName(), CommonColumnTypeEnum.VARCHAR);
        MYSQL_TO_COMMON_TYPE.put(MysqlType.JSON.getSourceName(), CommonColumnTypeEnum.JSON);
    }

    private static void initCommonTypeToMysql() {
        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.TINYINT, MysqlType.TINYINT.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.TINYINT, MysqlColumnTypeDescUtil.TINYINT);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.SMALLINT, MysqlType.SMALLINT.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.SMALLINT, MysqlColumnTypeDescUtil.SMALLINT);

        COMMON_TYPE_TO_MYSQL.put(
                CommonColumnTypeEnum.MEDIUMINT, MysqlType.MEDIUMINT.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.MEDIUMINT, MysqlColumnTypeDescUtil.MEDIUMINT);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.INTEGER, MysqlType.INTEGER.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.INTEGER, MysqlColumnTypeDescUtil.INTEGER);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.BIGINT, MysqlType.BIGINT.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.BIGINT, MysqlColumnTypeDescUtil.BIGINT);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.FLOAT, MysqlType.FLOAT.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.FLOAT, MysqlColumnTypeDescUtil.FLOAT);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.DOUBLE, MysqlType.DOUBLE.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.DOUBLE, MysqlColumnTypeDescUtil.DOUBLE);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.DECIMAL, MysqlType.DECIMAL.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.DECIMAL, MysqlColumnTypeDescUtil.DECIMAL);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.NUMERIC, MysqlType.NUMERIC.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.NUMERIC, MysqlColumnTypeDescUtil.NUMERIC);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.VARCHAR, MysqlType.VARCHAR.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.VARCHAR, MysqlColumnTypeDescUtil.VARCHAR);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.CHAR, MysqlType.CHAR.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.CHAR, MysqlColumnTypeDescUtil.CHAR);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.DATE, MysqlType.DATE.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.DATE, MysqlColumnTypeDescUtil.DATE);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.TIME, MysqlType.TIME.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.TIME, MysqlColumnTypeDescUtil.TIME);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.DATETIME, MysqlType.DATETIME.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.DATETIME, MysqlColumnTypeDescUtil.DATETIME);

        COMMON_TYPE_TO_MYSQL.put(
                CommonColumnTypeEnum.TIMESTAMP, MysqlType.TIMESTAMP.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.TIMESTAMP, MysqlColumnTypeDescUtil.TIMESTAMP);

        COMMON_TYPE_TO_MYSQL.put(
                CommonColumnTypeEnum.TIMESTAMP_WITH_TIME_ZONE, MysqlType.TIMESTAMP.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.TIMESTAMP_WITH_TIME_ZONE, MysqlColumnTypeDescUtil.TIMESTAMP);

        COMMON_TYPE_TO_MYSQL.put(
                CommonColumnTypeEnum.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                MysqlType.TIMESTAMP.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                MysqlColumnTypeDescUtil.TIMESTAMP);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.BIT, MysqlType.BIT.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(CommonColumnTypeEnum.BIT, MysqlColumnTypeDescUtil.BIT);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.BOOLEAN, MysqlType.BOOLEAN.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.BOOLEAN, MysqlColumnTypeDescUtil.BOOLEAN);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.BINARY, MysqlType.BINARY.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.BINARY, MysqlColumnTypeDescUtil.BINARY);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.BLOB, MysqlType.BLOB.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.BLOB, MysqlColumnTypeDescUtil.BLOB);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.CLOB, MysqlType.TEXT.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.CLOB, MysqlColumnTypeDescUtil.TEXT);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.JSON, MysqlType.JSON.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.JSON, MysqlColumnTypeDescUtil.JSON);

        COMMON_TYPE_TO_MYSQL.put(CommonColumnTypeEnum.XML, MysqlType.VARCHAR.getSourceName());
        COMMON_TYPE_TO_COLUMN_TYPE_DESC.put(
                CommonColumnTypeEnum.XML, MysqlColumnTypeDescUtil.VARCHAR);
    }

    private static void initAllColumnType() {
        Arrays.stream(MysqlType.values())
                .forEach(
                        i -> {
                            MYSQL_COLUMN_TYPE.add(i.name());
                        });
    }
}
