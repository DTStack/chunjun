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

package com.dtstack.flinkx.connector.postgresql.converter;

import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/22
 */
public class PostgresqlRawTypeConverter {

    /**
     * inspired by Postgresql doc. https://www.postgresql.org/docs/current/datatype.html
     *
     * @param type
     */
    public static DataType apply(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
                // Numeric Types
            case "SMALLINT":
            case "SMALLSERIAL":
            case "INT2":
                return DataTypes.SMALLINT();
            case "INT":
            case "INTEGER":
            case "SERIAL":
            case "INT4":
                return DataTypes.INT();
            case "BIGINT":
            case "BIGSERIAL":
            case "OID":
            case "INT8":
                return DataTypes.BIGINT();
            case "REAL":
            case "FLOAT4":
                return DataTypes.FLOAT();
            case "FLOAT":
            case "DOUBLE PRECISION":
            case "FLOAT8":
                return DataTypes.DOUBLE();
            case "DECIMAL":
            case "NUMERIC":
                //            case "MONEY":
                return DataTypes.DECIMAL(38, 18);

                // Character Types
            case "CHARACTER VARYING":
            case "VARCHAR":
            case "CHARACTER":
            case "CHAR":
            case "TEXT":
            case "NAME":
            case "BPCHAR":
                return DataTypes.STRING();

                // Binary Data Types
            case "BYTEA":
                return DataTypes.BYTES();

                // Date/Time Types
            case "TIMESTAMP":
            case "TIMESTAMPTZ":
                return DataTypes.TIMESTAMP();
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
            case "TIMETZ":
                return DataTypes.TIME();
                // interval 类型还不知道如何支持

                // Boolean Type
            case "BOOLEAN":
            case "BOOL":
                return DataTypes.BOOLEAN();

                // 以下类型无法支持
                // Enumerated Types

                // Geometric Types
                //            case "POINT":
                //            case "LINE":
                //            case "LSEG":
                //            case "BOX":
                //            case "PATH":
                //            case "POLYGON":
                //            case "CIRCLE":

                // Network Address Types

                // Bit String Types
                //            case "BIT":
                //                return DataTypes.BOOLEAN();
                //            case "BIT VARYING":
                //                return DataTypes.STRING();
                //
                //            case "XML":
                //                return DataTypes.STRING();
                //
                //                // JSON Types
                //            case "JSON":
                //            case "JSONB":
                //            case "JSONPATH":
                //                return DataTypes.STRING();

            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
