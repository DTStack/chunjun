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

package com.dtstack.chunjun.connector.iceberg.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.Locale;

public class ChunjunRowDataConvertMap implements MapFunction<RowData, RowData> {

    private static final long serialVersionUID = 3200338500689887544L;

    private final List<FieldConfig> columns;

    public ChunjunRowDataConvertMap(List<FieldConfig> columns) {
        this.columns = columns;
    }

    @Override
    public RowData map(RowData row) {
        if (row instanceof ColumnRowData) {
            GenericRowData convertedData = new GenericRowData(RowKind.INSERT, columns.size());

            /* 只有数据还原才有headers scn, schema, table, ts, opTime, type, before, after */
            boolean hasHeader = ((ColumnRowData) row).getHeaders() != null;

            for (FieldConfig column : columns) {
                int index;
                if (hasHeader) {
                    index = 7 + column.getIndex();
                } else {
                    index = column.getIndex();
                }
                String type = column.getType();
                Object value = getRowDataByType(row, type, index);
                convertedData.setField(column.getIndex(), value);
            }

            return convertedData;
        }
        return row;
    }

    private Object getRowDataByType(RowData data, String type, int index) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
            case "BIT":
                return data.getBoolean(index);
            case "TINYINT":
            case "TINYINT UNSIGNED":
            case "SMALLINT":
            case "SMALLINT UNSIGNED":
            case "MEDIUMINT":
            case "MEDIUMINT UNSIGNED":
            case "INT":
            case "INTEGER":
            case "INT24":
            case "INT UNSIGNED":
                return data.getInt(index);
            case "BIGINT":
            case "BIGINT UNSIGNED":
                return data.getLong(index);
            case "DECIMAL":
            case "DECIMAL UNSIGNED":
            case "NUMERIC":
                return data.getDecimal(index, 14, 2);
            case "REAL":
            case "FLOAT":
            case "FLOAT UNSIGNED":
                return data.getFloat(index);
            case "DOUBLE":
            case "DOUBLE UNSIGNED":
                return data.getDouble(index);
            case "CHAR":
            case "VARCHAR":
            case "STRING":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "JSON":
            case "ENUM":
            case "SET":
                return data.getString(index);
            case "DATE":
            case "TIME":
            case "TIMESTAMP":
            case "DATETIME":
                return data.getTimestamp(index, 0);

            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
