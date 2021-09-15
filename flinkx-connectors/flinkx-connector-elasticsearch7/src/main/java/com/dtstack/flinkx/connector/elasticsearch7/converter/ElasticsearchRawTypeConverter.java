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

package com.dtstack.flinkx.connector.elasticsearch7.converter;

import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 17:39
 */
public class ElasticsearchRawTypeConverter {

    public static DataType apply(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
                // Numeric Types
            case "BYTE":
            case "INT2":
                return DataTypes.SMALLINT();
            case "INT":
            case "INTEGER":
            case "INT4":
                return DataTypes.INT();
            case "BIGINT":
            case "INT8":
            case "LONG":
                return DataTypes.BIGINT();
            case "FLOAT4":
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE PRECISION":
            case "FLOAT8":
                return DataTypes.DOUBLE();
            case "DECIMAL":
            case "NUMERIC":
                return DataTypes.DECIMAL(38, 18);
            case "VARCHAR":
            case "TEXT":
                return DataTypes.STRING();
            case "BYTEA":
                return DataTypes.BYTES();
            case "TIMESTAMP":
            case "TIMESTAMPTZ":
                return DataTypes.TIMESTAMP();
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
            case "TIMETZ":
                return DataTypes.TIME();
                // interval 类型还不知道如何支持
            case "BOOLEAN":
            case "BOOL":
                return DataTypes.BOOLEAN();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
