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

package com.dtstack.flinkx.connector.dm.converter;

import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

/** @author kunni */
public class DmRawTypeConverter {

    /** inspired by dm doc. https://www.dameng.com/form/login/s/L3ZpZXdfNjEuaHRtbA%3D%3D.html */
    public static DataType apply(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "CHAR":
            case "CHARACTER":
            case "VARCHAR":
            case "VARCHAR2":
            case "CLOB":
            case "TEXT":
            case "LONG":
            case "LONGVARCHAR":
            case "ENUM":
            case "SET":
            case "JSON":
                return DataTypes.STRING();
            case "DECIMAL":
            case "NUMERIC":
            case "DEC":
            case "NUMBER":
                return DataTypes.DECIMAL(22, 6);
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "TINYINT":
            case "BYTE":
            case "BYTES":
                return DataTypes.TINYINT();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
            case "TINYBLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "GEOMETRY":
            case "IMAGE":
                // BYTES 底层调用的是VARBINARY最大长度
                return DataTypes.BYTES();
            case "REAL":
                return DataTypes.FLOAT();
            case "FLOAT":
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return DataTypes.DOUBLE();
            case "BIT":
                return DataTypes.BOOLEAN();
            case "YEAR":
                return DataTypes.INTERVAL(DataTypes.YEAR());
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "TIMESTAMP":
            case "DATETIME":
                return DataTypes.TIMESTAMP(6);
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
