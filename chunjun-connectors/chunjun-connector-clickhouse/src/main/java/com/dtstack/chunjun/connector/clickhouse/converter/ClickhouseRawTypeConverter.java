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

package com.dtstack.chunjun.connector.clickhouse.converter;

import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.sql.SQLException;
import java.util.Locale;

public class ClickhouseRawTypeConverter {

    /**
     * 将clickhouse数据库中的类型，转换成flink的DataType类型。 转换关系参考 ru.yandex.clickhouse.domain.ClickHouseDataType
     * 类里面的信息。
     *
     * @param type
     * @return
     * @throws SQLException
     */
    public static DataType apply(TypeConfig type) {
        switch (type.getType().toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "TINYINT":
            case "INT8":
            case "UINT8":
            case "SMALLINT":
            case "UINT16":
            case "INT16":
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
            case "UINT64":
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
            case "BINARY":
            case "STRUCT":
            case "VARCHAR":
            case "STRING":
            case "ENUM8":
            case "ENUM16":
            case "FIXEDSTRING":
            case "NESTED":
                return DataTypes.STRING();
            case "MAP(STRING,UINT8)":
            case "MAP(STRING,INT8)":
            case "MAP(STRING,UINT16)":
            case "MAP(STRING,INT16)":
            case "MAP(STRING,UINT32)":
            case "MAP(STRING,INT32)":
                return DataTypes.MAP(DataTypes.STRING(), DataTypes.INT());
            case "DATE":
                return DataTypes.DATE();
            case "TIME":
                return DataTypes.TIME();
            case "TIMESTAMP":
            case "DATETIME":
                return type.toTimestampDataType(0);
            case "DATETIME64":
                return type.toTimestampDataType(3);
            case "NOTHING":
            case "NULLABLE":
            case "NULL":
                return DataTypes.NULL();
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
