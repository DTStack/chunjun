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

package com.dtstack.chunjun.connector.sqlserver.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public class SqlserverJtdsRawTypeMapper {

    /**
     * Convert the data type in SqlServer to the DataType type in flink
     *
     * @param type
     * @return
     * @throws UnsupportedTypeException
     */
    public static DataType apply(TypeConfig type) throws UnsupportedTypeException {
        switch (type.getType()) {
            case "BIT":
                return DataTypes.BOOLEAN();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "INT IDENTITY":
                return DataTypes.INT();
            case "REAL":
                return DataTypes.FLOAT();
            case "FLOAT":
                return DataTypes.DOUBLE();
            case "DECIMAL":
            case "NUMERIC":
            case "MONEY":
            case "SMALLMONEY":
                return DataTypes.DECIMAL(1, 0);
            case "STRING":
            case "CHAR":
            case "VARCHAR":
            case "VARCHAR(MAX)":
            case "TEXT":
            case "XML":
            case "UNIQUEIDENTIFIER":
            case "NCHAR":
            case "NVARCHAR":
            case "NVARCHAR(MAX)":
            case "NTEXT":
            case "SYSNAME":
                return DataTypes.STRING();
            case "TIME":
                return DataTypes.TIME();
            case "DATETIME2":
                return DataTypes.TIMESTAMP(7);
            case "DATETIMEOFFSET":
                return DataTypes.TIMESTAMP_WITH_TIME_ZONE(7);
            case "DATE":
                return DataTypes.DATE();
            case "DATETIME":
                return DataTypes.TIMESTAMP(3);
            case "SMALLDATETIME":
                return DataTypes.TIMESTAMP(0);
            case "TIMESTAMP":
                return new AtomicDataType(new TimestampType(true, LogicalTypeRoot.BIGINT));
            case "BINARY":
            case "VARBINARY":
            case "IMAGE":
                return DataTypes.BYTES();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
