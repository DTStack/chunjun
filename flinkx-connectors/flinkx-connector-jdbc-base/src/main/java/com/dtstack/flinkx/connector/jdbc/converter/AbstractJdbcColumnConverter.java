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

package com.dtstack.flinkx.connector.jdbc.converter;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import io.vertx.core.json.JsonArray;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale;

/** Base class for all converters that convert between JDBC object and Flink internal object. */
public class AbstractJdbcColumnConverter
        extends AbstractRowConverter<ResultSet, JsonArray, FieldNamedPreparedStatement, String> {

    private static final long serialVersionUID = 1L;

    public AbstractJdbcColumnConverter(List<String> typeList) {
        super(typeList.size());
        for (int i = 0; i < typeList.size(); i++) {
            toInternalConverters[i] =
                    wrapIntoNullableInternalConverter(createInternalConverter(typeList.get(i)));
            toExternalConverters[i] =
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(typeList.get(i)), typeList.get(i));
        }
    }

    @Override
    protected SerializationConverter<FieldNamedPreparedStatement> wrapIntoNullableExternalConverter(
            SerializationConverter serializationConverter, String type) {
        return (val, index, statement) -> {
            if (((ColumnRowData) val).getField(index) == null) {
                statement.setObject(index, null);
            } else {
                serializationConverter.serialize(val, index, statement);
            }
        };
    }

    @Override
    public RowData toInternal(ResultSet resultSet) throws Exception {
        ColumnRowData data = new ColumnRowData(toInternalConverters.length);
        for (int i = 0; i < toInternalConverters.length; i++) {
            Object field = resultSet.getObject(i + 1);
            data.addField((AbstractBaseColumn) toInternalConverters[i].deserialize(field));
        }
        return data;
    }

    @Override
    public RowData toInternalLookup(JsonArray jsonArray) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = jsonArray.getValue(pos);
            genericRowData.setField(pos, toInternalConverters[pos].deserialize(field));
        }
        return genericRowData;
    }

    @Override
    public FieldNamedPreparedStatement toExternal(
            RowData rowData, FieldNamedPreparedStatement statement) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, statement);
        }
        return statement;
    }

    @Override
    protected DeserializationConverter<Object> createInternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BIT":
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case "TINYINT":
                return val -> new BigDecimalColumn(((Integer) val).byteValue());
            case "SMALLINT":
            case "MEDIUMINT":
            case "INT":
            case "INT24":
            case "INTEGER":
                return val -> new BigDecimalColumn((Integer) val);
            case "FLOAT":
            case "DOUBLE":
                return val -> new BigDecimalColumn((Double) val);
            case "REAL":
                return val -> new BigDecimalColumn((Float) val);
            case "LONG":
            case "BIGINT":
                return val -> new BigDecimalColumn((Long) val);
            case "DECIMAL":
            case "NUMERIC":
                return val -> new BigDecimalColumn((BigDecimal) val);
            case "CHAR":
            case "VARCHAR":
                return val -> new StringColumn((String) val);
            case "DATE":
                return val -> new BigDecimalColumn(((Date) val).toLocalDate().toEpochDay());
            case "TIME":
                return val ->
                        new BigDecimalColumn(((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L);
            case "TIMESTAMP":
            case "DATETIME":
                return val -> new TimestampColumn((Timestamp) val);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
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
}
