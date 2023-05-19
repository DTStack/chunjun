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

package com.dtstack.chunjun.connector.selectdbcloud.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class SelectdbcloudRawTypeMapper {

    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "BOOLEAN":
            case "BIT":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "TINYINT UNSIGNED":
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "SMALLINT UNSIGNED":
            case "MEDIUMINT":
            case "MEDIUMINT UNSIGNED":
            case "INT":
            case "INTEGER":
            case "INT24":
                return DataTypes.INT();
            case "INT UNSIGNED":
            case "BIGINT":
                return DataTypes.BIGINT();
            case "BIGINT UNSIGNED":
            case "DECIMAL":
            case "DECIMAL UNSIGNED":
            case "NUMERIC":
            case "DECIMALV2":
                return type.toDecimalDataType();
            case "REAL":
            case "FLOAT":
            case "FLOAT UNSIGNED":
                return DataTypes.FLOAT();
            case "DOUBLE":
            case "DOUBLE UNSIGNED":
                return DataTypes.DOUBLE();
            case "CHAR":
            case "VARCHAR":
            case "STRING":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "JSON":
            case "ENUM":
            case "SET":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "YEAR":
                return DataTypes.INTERVAL(DataTypes.YEAR());
            case "TIMESTAMP":
            case "DATETIME":
                return type.toTimestampDataType(0);
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "BINARY":
            case "VARBINARY":
            case "GEOMETRY":
                // BYTES 底层调用的是VARBINARY最大长度
                return DataTypes.BYTES();
            case "NULL_TYPE":
            case "NULL":
                return DataTypes.NULL();
            case "HLL":
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
