/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.vertica11.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.SQLException;

public class Vertica11RawTypeConverter {

    private static final Integer MAX_BINARY_LENGTH = 65000; // vertica binary 最大长度
    private static final Integer MAX_VARBINARY_LENGTH = 65000; // vertica varbinary 最大长度
    private static final Integer MAX_LONG_VARBINARY_LENGTH =
            32000000; // vertica long varbinary 最大长度
    /**
     * 将Vertica数据库中的类型，转换成flink的DataType类型。
     *
     * @param type
     * @return
     * @throws SQLException
     */
    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "VARCHAR":
            case "CHAR":
            case "LONG VARCHAR":
                return DataTypes.STRING();
            case "BINARY":
                return DataTypes.BINARY(MAX_BINARY_LENGTH);
            case "VARBINARY":
                return DataTypes.BINARY(MAX_VARBINARY_LENGTH);
            case "LONG VARBINARY":
                return DataTypes.BINARY(MAX_LONG_VARBINARY_LENGTH);
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "INTEGER":
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT":
            case "DOUBLE":
            case "NUMERIC":
            case "DECIMAL":
                return DataTypes.DECIMAL(38, 18);
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "TIMETZ":
            case "TIMESTAMP":
            case "DATETIME":
                return DataTypes.TIMESTAMP(0);
            case "TIMESTAMPTZ":
                return DataTypes.TIMESTAMP_WITH_TIME_ZONE();
            case "GEOGRAPHY":
            case "GEOMETRY":
            case "INTERVAL MONTH":
            case "INTERVAL DAY":
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
