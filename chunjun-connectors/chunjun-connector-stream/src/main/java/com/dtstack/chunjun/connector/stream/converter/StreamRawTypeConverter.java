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

package com.dtstack.chunjun.connector.stream.converter;

import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

public class StreamRawTypeConverter {

    public static DataType apply(String type) throws UnsupportedTypeException {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "TINYINT":
            case "YEAR":
                return DataTypes.TINYINT();
            case "SMALLINT":
            case "SHORT":
                return DataTypes.SMALLINT();
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "ID":
            case "LONG":
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "DECIMAL":
                return DataTypes.DECIMAL(38, 18);
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "DATETIME":
                return DataTypes.TIMESTAMP(0);
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
            case "BOOLEAN":
                return DataTypes.BOOLEAN();

            default:
                return DataTypes.STRING();
        }
    }
}
