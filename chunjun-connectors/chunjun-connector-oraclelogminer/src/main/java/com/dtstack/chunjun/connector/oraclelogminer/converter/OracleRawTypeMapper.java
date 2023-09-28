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

package com.dtstack.chunjun.connector.oraclelogminer.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.SQLException;

public class OracleRawTypeMapper {

    /**
     * 将Oracle数据库中的类型，转换成flink的DataType类型。
     *
     * @param type
     * @return
     * @throws SQLException
     */
    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "BINARY_DOUBLE":
                return DataTypes.DOUBLE();
            case "CHAR":
            case "VARCHAR":
            case "VARCHAR2":
            case "NCHAR":
            case "NVARCHAR2":
            case "LONG":
            case "RAW":
            case "LONG RAW":
            case "BLOB":
            case "CLOB":
            case "NCLOB":
            case "INTERVAL YEAR":
            case "INTERVAL DAY":
                return DataTypes.STRING();
            case "INT":
            case "INTEGER":
                return DataTypes.INT();
            case "NUMBER":
                return DataTypes.BIGINT();
            case "DECIMAL":
                return DataTypes.DECIMAL(1, 0);
            case "DATE":
                return DataTypes.DATE();
            case "BINARY_FLOAT":
            case "FLOAT":
                return DataTypes.FLOAT();
            case "BFILE":
            case "XMLTYPE":
                throw new UnsupportedTypeException(type);
            default:
                if (type.getType().startsWith("TIMESTAMP")) {
                    if (type.getType().contains("TIME ZONE")) {
                        return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
                    } else {
                        return DataTypes.TIMESTAMP();
                    }
                }
                throw new UnsupportedTypeException(type);
        }
    }
}
