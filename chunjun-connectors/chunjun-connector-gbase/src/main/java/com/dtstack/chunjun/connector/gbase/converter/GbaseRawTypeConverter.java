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

package com.dtstack.chunjun.connector.gbase.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class GbaseRawTypeConverter {

    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "BIT":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
            case "MEDIUMINT":
            case "INT":
            case "INTEGER":
            case "INT24":
            case "SERIAL":
                return DataTypes.INT();
            case "BIGINT":
            case "INT8":
            case "BIGSERIAL":
            case "SERIAL8":
                return DataTypes.BIGINT();
            case "REAL":
            case "FLOAT":
            case "SMALLFLOAT":
                return DataTypes.FLOAT();
            case "DECIMAL":
            case "DEC":
            case "NUMERIC":
            case "MONEY":
                // TODO 精度应该可以动态传进来？
                return DataTypes.DECIMAL(38, 18);
            case "DOUBLE":
            case "PRECISION":
                return DataTypes.DOUBLE();
            case "CHAR":
            case "VARCHAR":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LVARCHAR":
            case "LONGTEXT":
            case "JSON":
            case "ENUM":
            case "CHARACTER":
            case "VARYING":
            case "NCHAR":
            case "SET":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "YEAR":
                return DataTypes.INTERVAL(DataTypes.YEAR());
            case "TIME":
                return DataTypes.TIME();
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            case "DATETIME":
                return DataTypes.TIMESTAMP(5);
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "BINARY":
            case "VARBINARY":
            case "GEOMETRY":
                // BYTES 底层调用的是VARBINARY最大长度
                return DataTypes.BYTES();

            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
