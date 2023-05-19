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

package com.dtstack.chunjun.connector.sybase.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class SybaseRawTypeMapper {
    public static DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "BIGINT":
            case "UNSIGNED INT":
                return DataTypes.BIGINT();
            case "INT":
            case "INTEGER":
            case "UNSIGNED SMALLINT":
                return DataTypes.INT();
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "UNSIGNED BIGINT":
                return DataTypes.DECIMAL(20, 0);
            case "NUMERIC":
            case "DECIMAL":
                return DataTypes.DECIMAL(38, 18);
            case "NUMERIC IDENTITY":
                return DataTypes.DECIMAL(38, 0);
            case "FLOAT":
            case "REAL":
                return DataTypes.FLOAT();
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return DataTypes.DOUBLE();
            case "SMALLMONEY":
                return DataTypes.DECIMAL(10, 4);
            case "MONEY":
                return DataTypes.DECIMAL(19, 4);
            case "SMALLDATETIME":
                return DataTypes.TIMESTAMP(0);
            case "DATETIME":
            case "BIGDATETIME":
                return DataTypes.TIMESTAMP(3);
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
            case "BIGTIME":
                return DataTypes.TIME();
            case "CHAR":
            case "VARCHAR":
            case "UNICHAR":
            case "UNIVARCHAR":
            case "NCHAR":
            case "NVARCHAR":
            case "TEXT":
            case "UNITEXT":
            case "LONGSYSNAME":
            case "STRING":
            case "SYSNAME":
                return DataTypes.STRING();
            case "BINARY":
            case "TIMESTAMP":
            case "VARBINARY":
            case "IMAGE":
                return DataTypes.BYTES();
            case "BIT":
                return DataTypes.BOOLEAN();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
