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

package com.dtstack.chunjun.ddl.convent.mysql.type;

import com.dtstack.chunjun.cdc.ddl.definition.ColumnTypeDesc;

public class MysqlColumnTypeDescUtil {

    public static ColumnTypeDesc SERIAL = new ColumnTypeDesc(MysqlType.SERIAL, false);
    public static ColumnTypeDesc TINYINT = new ColumnTypeDesc(MysqlType.TINYINT, true);
    public static ColumnTypeDesc SMALLINT = new ColumnTypeDesc(MysqlType.SMALLINT, true);
    public static ColumnTypeDesc MEDIUMINT = new ColumnTypeDesc(MysqlType.MEDIUMINT, true);
    public static ColumnTypeDesc INTEGER = new ColumnTypeDesc(MysqlType.INTEGER, true);
    public static ColumnTypeDesc INT = new ColumnTypeDesc(MysqlType.INT, true);
    public static ColumnTypeDesc BIGINT = new ColumnTypeDesc(MysqlType.BIGINT, true);
    public static ColumnTypeDesc VARCHAR =
            new ColumnTypeDesc(MysqlType.VARCHAR, 255, null, 21844, null, 1, null, true);
    public static ColumnTypeDesc CHAR = new ColumnTypeDesc(MysqlType.CHAR, true);
    public static ColumnTypeDesc FLOAT = new ColumnTypeDesc(MysqlType.FLOAT, true);
    public static ColumnTypeDesc REAL = new ColumnTypeDesc(MysqlType.REAL, true);
    public static ColumnTypeDesc DOUBLE = new ColumnTypeDesc(MysqlType.DOUBLE, true);
    public static ColumnTypeDesc DECIMAL =
            new ColumnTypeDesc(MysqlType.DECIMAL, null, null, 65, 30, 1, 1, true);
    public static ColumnTypeDesc NUMERIC = new ColumnTypeDesc(MysqlType.NUMERIC, true);
    public static ColumnTypeDesc BIT = new ColumnTypeDesc(MysqlType.BIT, true);
    public static ColumnTypeDesc BOOLEAN = new ColumnTypeDesc(MysqlType.BOOLEAN, false);
    public static ColumnTypeDesc DATE = new ColumnTypeDesc(MysqlType.DATE, false);
    public static ColumnTypeDesc TIME = new ColumnTypeDesc(MysqlType.TIME, false);
    public static ColumnTypeDesc DATETIME = new ColumnTypeDesc(MysqlType.DATETIME, true);
    public static ColumnTypeDesc TIMESTAMP = new ColumnTypeDesc(MysqlType.TIMESTAMP, true);
    public static ColumnTypeDesc BINARY = new ColumnTypeDesc(MysqlType.BINARY, true);
    public static ColumnTypeDesc VARBINARY = new ColumnTypeDesc(MysqlType.VARBINARY, true);
    public static ColumnTypeDesc TEXT = new ColumnTypeDesc(MysqlType.TEXT, true);
    public static ColumnTypeDesc TINYTEXT = new ColumnTypeDesc(MysqlType.TINYTEXT, true);
    public static ColumnTypeDesc MEDIUMTEXT = new ColumnTypeDesc(MysqlType.MEDIUMTEXT, true);
    public static ColumnTypeDesc LONG = new ColumnTypeDesc(MysqlType.LONG, true);
    public static ColumnTypeDesc LONGTEXT = new ColumnTypeDesc(MysqlType.LONGTEXT, true);
    public static ColumnTypeDesc TINYBLOB = new ColumnTypeDesc(MysqlType.TINYBLOB, true);
    public static ColumnTypeDesc MEDIUMBLOB = new ColumnTypeDesc(MysqlType.MEDIUMBLOB, true);
    public static ColumnTypeDesc BLOB = new ColumnTypeDesc(MysqlType.BLOB, true);
    public static ColumnTypeDesc LONGBLOB = new ColumnTypeDesc(MysqlType.LONGBLOB, true);
    public static ColumnTypeDesc YEAR = new ColumnTypeDesc(MysqlType.YEAR, false);
    public static ColumnTypeDesc ENUM = new ColumnTypeDesc(MysqlType.ENUM, false);
    public static ColumnTypeDesc JSON = new ColumnTypeDesc(MysqlType.JSON, false);
    public static ColumnTypeDesc POINT = new ColumnTypeDesc(MysqlType.POINT, false);
    public static ColumnTypeDesc GEOMETRY = new ColumnTypeDesc(MysqlType.GEOMETRY, false);
    public static ColumnTypeDesc LINESTRING = new ColumnTypeDesc(MysqlType.LINESTRING, false);
    public static ColumnTypeDesc POLYGON = new ColumnTypeDesc(MysqlType.POLYGON, false);
    public static ColumnTypeDesc MULTIPOINT = new ColumnTypeDesc(MysqlType.MULTIPOINT, false);
    public static ColumnTypeDesc MULTILINESTRING =
            new ColumnTypeDesc(MysqlType.MULTILINESTRING, false);
    public static ColumnTypeDesc MULTIPOLYGON = new ColumnTypeDesc(MysqlType.MULTIPOLYGON, false);
    public static ColumnTypeDesc GEOMCOLLECTION =
            new ColumnTypeDesc(MysqlType.GEOMCOLLECTION, false);
}
