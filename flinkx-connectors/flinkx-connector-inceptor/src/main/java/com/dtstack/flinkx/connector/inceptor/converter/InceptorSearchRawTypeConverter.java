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

package com.dtstack.flinkx.connector.inceptor.converter;

import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

/** @author liuliu 2022/2/22 */
public class InceptorSearchRawTypeConverter {

    public static DataType apply(String type) {
        type = type.toUpperCase(Locale.ENGLISH);
        switch (type) {
            case "STRING":
                return DataTypes.STRING();
            case "INT":
                return DataTypes.INT();
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT":
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "DATE":
                return DataTypes.DATE();
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP(9);
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
