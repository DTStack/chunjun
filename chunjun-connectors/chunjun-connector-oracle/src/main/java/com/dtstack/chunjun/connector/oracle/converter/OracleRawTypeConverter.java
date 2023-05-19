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

package com.dtstack.chunjun.connector.oracle.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.sql.SQLException;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class OracleRawTypeConverter {

    private static final String TIMESTAMP = "^TIMESTAMP\\(\\d+\\)";
    private static final Predicate<String> TIMESTAMP_PREDICATE =
            Pattern.compile(TIMESTAMP).asPredicate();

    /**
     * 将Oracle数据库中的类型，转换成flink的DataType类型。
     *
     * @param type typeConfig
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
                return DataTypes.CHAR(OracleSqlConverter.CLOB_LENGTH - 1);
            case "ROWID":
            case "UROWID":
            case "STRING":
            case "VARCHAR":
            case "VARCHAR2":
            case "NCHAR":
            case "NVARCHAR2":
            case "LONG":
                return DataTypes.VARCHAR(OracleSqlConverter.CLOB_LENGTH - 1);
            case "CLOB":
            case "NCLOB":
                return new AtomicDataType(new ClobType(true, LogicalTypeRoot.VARCHAR));
                //            case "XMLTYPE":
            case "INT":
            case "INTEGER":
            case "NUMBER":
            case "FLOAT":
                return DataTypes.DECIMAL(38, 18);
            case "DECIMAL":
                return type.toDecimalDataType();
            case "DATE":
                return DataTypes.TIMESTAMP(0);
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            case "RAW":
            case "LONG RAW":
                return DataTypes.BYTES();
            case "BLOB":
                return new AtomicDataType(new BlobType(true, LogicalTypeRoot.VARBINARY));
            case "BINARY_FLOAT":
                return DataTypes.FLOAT();
            case "INTERVAL YEAR TO MONTH":
            case "INTERVALYM":
                return type.toIntervalYearMonthDataType();
            case "INTERVAL DAY TO SECOND":
            case "INTERVALDS":
                return type.toIntervalDaySecondDataType();
            case "TIMESTAMP WITH TIME ZONE":
                return type.toZonedTimestampDataType();
            case "TIMESTAMP WITH LOCAL TIME ZONE":
                return type.toLocalZonedTimestampDataType();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
