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

package com.dtstack.flinkx.connector.oracle.converter;

import com.dtstack.flinkx.throwable.UnsupportedTypeException;
import com.dtstack.flinkx.util.ColumnTypeUtil;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.sql.SQLException;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleRawTypeConverter {

    private static final String TIMESTAMP = "^TIMESTAMP\\(\\d+\\)";
    private static final Predicate<String> TIMESTAMP_PREDICATE =
            Pattern.compile(TIMESTAMP).asPredicate();

    /**
     * 将Oracle数据库中的类型，转换成flink的DataType类型。
     *
     * @param type
     * @return
     * @throws SQLException
     */
    public static DataType apply(String type) {
        ColumnTypeUtil.DecimalInfo decimalInfo = null;
        if (ColumnTypeUtil.isDecimalType(type)) {
            decimalInfo = ColumnTypeUtil.getDecimalInfo(type, null);
            if (decimalInfo != null) {
                type = ColumnTypeUtil.TYPE_NAME;
            }
        }
        switch (type.toUpperCase(Locale.ENGLISH)) {
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
                return DataTypes.VARCHAR(OracleRowConverter.CLOB_LENGTH - 1);
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
                return DataTypes.DECIMAL(decimalInfo.getPrecision(), decimalInfo.getScale());
            case "DATE":
                return DataTypes.DATE();
            case "RAW":
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            case "LONG RAW":
                return DataTypes.BYTES();
            case "BLOB":
                return new AtomicDataType(new BlobType(true, LogicalTypeRoot.VARBINARY));
            case "BINARY_FLOAT":
                return DataTypes.FLOAT();
            default:
                if (TIMESTAMP_PREDICATE.test(type)) {
                    return DataTypes.TIMESTAMP();
                } else if (type.startsWith("INTERVAL")) {
                    return DataTypes.STRING();
                }
                throw new UnsupportedTypeException(type);
        }
    }
}
