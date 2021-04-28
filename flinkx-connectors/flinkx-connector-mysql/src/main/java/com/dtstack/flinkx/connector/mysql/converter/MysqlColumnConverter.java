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

package com.dtstack.flinkx.connector.mysql.converter;

import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;

/**
 * @author chuixue
 * @create 2021-04-27 11:46
 * @description
 */
public class MysqlColumnConverter extends MysqlRowConverter {

    public MysqlColumnConverter(List<String> typeList) {
        super.toInternalConverters = new DeserializationConverter[typeList.size()];
        super.toExternalConverters = new SerializationConverter[typeList.size()];
        for (int i = 0; i < typeList.size(); i++) {
            toInternalConverters[i] =
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(typeList.get(i), i + 1), i + 1);
            toExternalConverters[i] =
                    wrapIntoNullableExternalConverter(createExternalConverter(typeList.get(i)));
        }
    }

    protected DeserializationConverter<ResultSet> createInternalConverter(String type, int index) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BIT":
                return val -> new BooleanColumn(val.getBoolean(index) ? 1 : 0);
            case "TINYINT":
                return val -> new BigDecimalColumn(val.getByte(index));
            case "SMALLINT":
            case "MEDIUMINT":
            case "INT":
            case "INT24":
            case "INTEGER":
                return val -> new BigDecimalColumn(val.getInt(index));
            case "FLOAT":
            case "DOUBLE":
                return val -> new BigDecimalColumn(val.getDouble(index));
            case "REAL":
                return val -> new BigDecimalColumn(val.getFloat(index));
            case "LONG":
            case "BIGINT":
                return val -> new BigDecimalColumn(val.getLong(index));
            case "DECIMAL":
            case "NUMERIC":
                return val -> new BigDecimalColumn(val.getBigDecimal(index));
            case "CHAR":
            case "VARCHAR":
                return val -> new StringColumn(val.getString(index));
            case "DATE":
                return val -> new BigDecimalColumn(val.getDate(index).toLocalDate().toEpochDay());
            case "TIME":
                return val ->
                        new BigDecimalColumn(
                                val.getTime(index).toLocalTime().toNanoOfDay() / 1_000_000L);
            case "TIMESTAMP":
            case "DATETIME":
                return val -> new TimestampColumn(val.getTimestamp(index));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    protected SerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
            String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BIT":
                return (val, index, statement) ->
                        statement.setBoolean(
                                index, ((ColumnRowData) val).getField(index).asBoolean());
            case "TINYINT":
                return (val, index, statement) -> statement.setByte(index, val.getByte(index));
            case "SMALLINT":
            case "MEDIUMINT":
            case "INT":
            case "INT24":
            case "INTEGER":
                return (val, index, statement) ->
                        statement.setInt(index, ((ColumnRowData) val).getField(index).asInt());
            case "FLOAT":
            case "DOUBLE":
                return (val, index, statement) ->
                        statement.setDouble(
                                index, ((ColumnRowData) val).getField(index).asDouble());
            case "REAL":
                return (val, index, statement) ->
                        statement.setFloat(index, ((ColumnRowData) val).getField(index).asFloat());
            case "LONG":
            case "BIGINT":
                return (val, index, statement) ->
                        statement.setLong(index, ((ColumnRowData) val).getField(index).asLong());
            case "DECIMAL":
            case "NUMERIC":
                return (val, index, statement) ->
                        statement.setBigDecimal(
                                index, ((ColumnRowData) val).getField(index).asBigDecimal());
            case "CHAR":
            case "VARCHAR":
                return (val, index, statement) ->
                        statement.setString(
                                index, ((ColumnRowData) val).getField(index).asString());
            case "DATE":
                return (val, index, statement) ->
                        statement.setDate(
                                index,
                                Date.valueOf(
                                        LocalDate.ofEpochDay(
                                                ((ColumnRowData) val).getField(index).asInt())));
            case "TIME":
                return (val, index, statement) ->
                        statement.setTime(
                                index,
                                Time.valueOf(
                                        LocalTime.ofNanoOfDay(
                                                ((ColumnRowData) val).getField(index).asInt()
                                                        * 1_000_000L)));
            case "TIMESTAMP":
            case "DATETIME":
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, ((ColumnRowData) val).getField(index).asTimestamp());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    public RowData toInternal(ResultSet resultSet) throws Exception {
        ColumnRowData data = new ColumnRowData(toInternalConverters.length);
        for (int i = 0; i < toInternalConverters.length; i++) {
            data.addField((AbstractBaseColumn) toInternalConverters[i].deserialize(resultSet));
        }
        return data;
    }

    @Override
    public FieldNamedPreparedStatement toExternal(
            RowData rowData, FieldNamedPreparedStatement output) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, output);
        }
        return output;
    }
}
