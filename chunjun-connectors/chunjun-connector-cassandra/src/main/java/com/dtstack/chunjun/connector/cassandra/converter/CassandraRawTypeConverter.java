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

package com.dtstack.chunjun.connector.cassandra.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class CassandraRawTypeConverter {

    /**
     * @param type cassandra type
     * @return flink type
     */
    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
            case "INT2":
                return DataTypes.SMALLINT();
            case "INT":
            case "INTEGER":
            case "COUNTER":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DECIMAL":
            case "NUMERIC":
            case "VARINT":
                return DataTypes.DECIMAL(38, 18);
            case "DOUBLE":
                return DataTypes.DOUBLE();
            case "ASCII":
            case "VARCHAR":
            case "TEXT":
            case "INET":
            case "UUID":
            case "TIMEUUID":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
            case "TIME_WITHOUT_TIME_ZONE":
                return DataTypes.TIME();
            case "TIMESTAMP":
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
                return DataTypes.TIMESTAMP();
            case "BLOB":
            case "JSONB":
                return DataTypes.BYTES();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
