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

package com.dtstack.chunjun.connector.hbase.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class HBaseRawTypeMapper implements RawTypeMapper {
    public static final HBaseRawTypeMapper INSTANCE = new HBaseRawTypeMapper();

    private HBaseRawTypeMapper() {}

    public DataType apply(TypeConfig type) {
        switch (type.getType()) {
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
            case "INT8":
            case "UINT8":
                return DataTypes.TINYINT();
            case "SMALLINT":
            case "UINT16":
            case "INT16":
                return DataTypes.SMALLINT();
            case "INTEGER":
            case "INTERVALYEAR":
            case "INTERVALQUARTER":
            case "INTERVALMONTH":
            case "INTERVALWEEK":
            case "INTERVALDAY":
            case "INTERVALHOUR":
            case "INTERVALMINUTE":
            case "INTERVALSECOND":
            case "INT32":
            case "INT":
                return DataTypes.INT();
            case "UINT32":
            case "UINT64":
            case "INT64":
            case "BIGINT":
                return DataTypes.BIGINT();
            case "FLOAT":
            case "FLOAT32":
                return DataTypes.FLOAT();
            case "DECIMAL":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "DEC":
                return type.toDecimalDataType();
            case "DOUBLE":
            case "FLOAT64":
                return DataTypes.DOUBLE();
            case "UUID":
            case "COLLECTION":
            case "BLOB":
            case "LONGTEXT":
            case "TINYTEXT":
            case "TEXT":
            case "CHAR":
            case "MEDIUMTEXT":
            case "TINYBLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "STRUCT":
            case "VARCHAR":
            case "STRING":
            case "ENUM8":
            case "ENUM16":
            case "FIXEDSTRING":
            case "NESTED":
                return DataTypes.STRING();
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "TIMESTAMP":
                return type.toTimestampDataType();
            case "DATETIME":
                return DataTypes.TIMESTAMP(3);
            case "BYTES":
            case "BINARY":
                return DataTypes.BYTES();
            case "NOTHING":
            case "NULLABLE":
            case "NULL":
                return DataTypes.NULL();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
