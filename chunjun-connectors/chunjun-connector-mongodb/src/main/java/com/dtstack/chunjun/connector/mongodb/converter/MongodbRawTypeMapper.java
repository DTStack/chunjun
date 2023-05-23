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

package com.dtstack.chunjun.connector.mongodb.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import com.google.common.collect.Lists;

import java.util.List;

public class MongodbRawTypeMapper {
    public static List<String> TYPE =
            Lists.newArrayList(
                    "INT",
                    "LONG",
                    "DOUBLE",
                    "DECIMAL",
                    "OBJECTID",
                    "STRING",
                    "BINDATA",
                    "DATE",
                    "TIMESTAMP",
                    "BOOLEAN",
                    "BOOL");

    /**
     * Inspired by MongoDB doc. https://docs.mongodb.com/manual/reference/bson-types/
     *
     * @param type
     */
    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "LONG":
                return DataTypes.BIGINT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "DECIMAL":
            case "NUMBER":
                return type.toDecimalDataType();
            case "OBJECTID":
            case "STRING":
            case "ARRAYS":
            case "NULL":
                return DataTypes.STRING();

            case "BINDATA":
                return DataTypes.BYTES();

            case "DATE":
                return DataTypes.DATE();
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();

            case "BOOLEAN":
            case "BOOL":
                return DataTypes.BOOLEAN();

            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
