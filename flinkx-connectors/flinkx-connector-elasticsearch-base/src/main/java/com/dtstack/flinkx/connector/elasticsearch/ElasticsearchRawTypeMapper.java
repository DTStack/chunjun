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

package com.dtstack.flinkx.connector.elasticsearch;

import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

public class ElasticsearchRawTypeMapper {

    /**
     * Inspired by
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-data-types.html
     *
     * @param type
     * @return
     */
    public static DataType apply(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
                // Numeric Types
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "BYTE":
                return DataTypes.TINYINT();
            case "SHORT":
                return DataTypes.SMALLINT();
            case "INTEGER":
                return DataTypes.INT();
            case "LONG":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "TEXT":
                return DataTypes.STRING();
            case "BINARY":
                return DataTypes.BYTES();
            case "DATE":
                return DataTypes.TIMESTAMP();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
