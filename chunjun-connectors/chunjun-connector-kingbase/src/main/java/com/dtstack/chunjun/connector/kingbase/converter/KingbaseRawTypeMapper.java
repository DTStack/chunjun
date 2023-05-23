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

package com.dtstack.chunjun.connector.kingbase.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.SQLException;

public class KingbaseRawTypeMapper {

    /**
     * @param type
     * @return
     * @throws SQLException
     */
    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "BIT":
            case "BOOL":
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "SMALLINT":
            case "INT2":
                return DataTypes.SMALLINT();
            case "INT":
            case "INT4":
            case "INTEGER":
                return DataTypes.INT();
            case "INT8":
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT4":
            case "REAL":
                return DataTypes.FLOAT();
            case "DECIMAL":
            case "NUMERIC":
                return DataTypes.DECIMAL(38, 18);
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "FLOAT8":
                return DataTypes.DOUBLE();
            case "BPCHAR":
            case "CHAR":
            case "VARCHAR":
            case "TEXT":
            case "JSON":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            case "BLOB":
            case "JSONB":
                return DataTypes.BYTES();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
