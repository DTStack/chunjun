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

package com.dtstack.chunjun.connector.kudu.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class KuduRawTypeMapper {

    /**
     * @param type kudu original type
     * @return the type of flink table
     */
    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "INT8":
            case "TINYINT":
                return DataTypes.TINYINT();
            case "BYTES":
            case "BINARY":
                return DataTypes.BINARY(Integer.MAX_VALUE);
            case "INT16":
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "INT":
            case "INT32":
            case "INTEGER":
                return DataTypes.INT();
            case "INT64":
            case "BIGINT":
            case "LONG":
            case "UNIXTIME_MICROS":
                return DataTypes.BIGINT();
            case "BOOL":
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "DECIMAL":
                return type.toDecimalDataType();
            case "VARCHAR":
            case "STRING":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
