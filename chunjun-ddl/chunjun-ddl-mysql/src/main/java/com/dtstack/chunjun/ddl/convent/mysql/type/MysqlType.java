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

import com.dtstack.chunjun.cdc.ddl.ColumnType;

public enum MysqlType implements ColumnType {
    SERIAL("SERIAL"),
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    MEDIUMINT("MEDIUMINT"),
    INTEGER("INTEGER"),
    INT("INT"),
    BIGINT("BIGINT"),
    VARCHAR("VARCHAR"),
    CHAR("CHAR"),
    FLOAT("FLOAT"),
    REAL("REAL"),
    DOUBLE("DOUBLE"),
    DECIMAL("DECIMAL"),
    NUMERIC("NUMERIC"),
    BIT("BIT"),
    BOOLEAN("BOOLEAN"),
    DATE("DATE"),
    TIME("TIME"),
    DATETIME("DATETIME"),
    TIMESTAMP("TIMESTAMP"),
    BINARY("BINARY"),
    VARBINARY("VARBINARY"),
    TEXT("TEXT"),
    TINYTEXT("TINYTEXT"),
    MEDIUMTEXT("MEDIUMTEXT"),
    LONG("LONG"),
    LONGTEXT("LONGTEXT"),
    TINYBLOB("TINYBLOB"),
    MEDIUMBLOB("MEDIUMBLOB"),
    BLOB("BLOB"),
    LONGBLOB("LONGBLOB"),
    YEAR("YEAR"),
    ENUM("ENUM"),
    JSON("JSON"),
    POINT("POINT"),
    GEOMETRY("GEOMETRY"),
    LINESTRING("LINESTRING"),
    POLYGON("POLYGON"),
    MULTIPOINT("MULTIPOINT"),
    MULTILINESTRING("MULTILINESTRING"),
    MULTIPOLYGON("MULTIPOLYGON"),
    GEOMCOLLECTION("GEOMCOLLECTION"),
    ;

    private String sourceName;

    MysqlType(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public String getSourceName() {
        return sourceName;
    }
}
