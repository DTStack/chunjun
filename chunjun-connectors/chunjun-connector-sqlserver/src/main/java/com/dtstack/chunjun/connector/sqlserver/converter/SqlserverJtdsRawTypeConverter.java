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

import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Locale;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/5/19 14:26
 */
public class SqlserverJtdsRawTypeConverter {

    /**
     * Convert the data type in SqlServer to the DataType type in flink
     *
     * @param type
     * @return
     * @throws UnsupportedTypeException
     */
    public static DataType apply(String type) throws UnsupportedTypeException {
        // like numeric() identity, decimal() identity
        if (type.contains("identity")) {
            type = type.replace("identity", "").trim();
            if (type.endsWith("()")) {
                type = type.replace("()", "").trim();
            }
        }
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BIT":
                return DataTypes.BOOLEAN();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "TINYINT":
                return DataTypes.TINYINT();
            case "INT":
            case "SMALLINT":
            case "INT IDENTITY":
                return DataTypes.INT();
            case "REAL":
                return DataTypes.FLOAT();
            case "FLOAT":
                return DataTypes.DOUBLE();
            case "DECIMAL":
            case "NUMERIC":
                return DataTypes.DECIMAL(1, 0);
            case "CHAR":
            case "VARCHAR":
            case "VARCHAR(MAX)":
            case "TEXT":
            case "XML":
                return DataTypes.STRING();
            case "NCHAR":
            case "NVARCHAR":
            case "NVARCHAR(MAX)":
            case "NTEXT":
                return DataTypes.STRING();
            case "TIME":
                return DataTypes.TIME();
            case "DATETIME2":
                return DataTypes.TIMESTAMP(7);
            case "DATETIMEOFFSET":
                return DataTypes.TIMESTAMP_WITH_TIME_ZONE(7);
            case "UNIQUEIDENTIFIER":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "DATETIME":
                return DataTypes.TIMESTAMP(3);
            case "SMALLDATETIME":
                return DataTypes.TIMESTAMP(0);
            case "BINARY":
            case "VARBINARY":
            case "IMAGE":
            case "TIMESTAMP":
                return DataTypes.BYTES();
            case "MONEY":
            case "SMALLMONEY":
                return DataTypes.DECIMAL(1, 0);
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
