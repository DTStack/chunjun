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
package com.dtstack.chunjun.connector.db2.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

public class Db2RawTypeConverter {

    /**
     * @param type db2 type
     * @return flink type
     */
    public static DataType apply(TypeConfig type) {
        switch (type.getType().toUpperCase(Locale.ENGLISH)) {
            case "CHAR":
            case "VARCHAR":
            case "CLOB":
            case "XML":
            case "LONG VARCHAR":
                return DataTypes.STRING();
            case "SMALLINT":
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "REAL":
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "DECIMAL":
            case "NUMERIC":
            case "DECFLOAT":
                return DataTypes.DECIMAL(1, 0);
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return type.toTimeDataType(0);
            case "TIMESTAMP":
            case "DATETIME":
                return type.toTimestampDataType(6);
            case "BLOB":
            case "BOOLEAN":
                return DataTypes.BYTES();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
