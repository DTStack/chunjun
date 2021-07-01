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

package com.dtstack.flinkx.connector.cassandra.converter;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.ByteColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;
import com.dtstack.flinkx.util.DateUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.List;
import java.util.Locale;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraColumnConverter
        extends AbstractRowConverter<ResultSet, Row, BoundStatement, String> {

    private final List<String> columnNameList;

    public CassandraColumnConverter(RowType rowType, List<String> columnNameList) {
        super(rowType);
        this.columnNameList = columnNameList;
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters[i] =
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i).getTypeRoot().name()));
            toExternalConverters[i] =
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(rowType.getTypeAt(i).getTypeRoot().name()),
                            rowType.getTypeAt(i).getTypeRoot().name());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ISerializationConverter<BoundStatement> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, String type) {
        return (val, index, statement) -> {
            if (((ColumnRowData) val).getField(index) == null || val.isNullAt(index)) {
                statement.setToNull(index);
            } else {
                serializationConverter.serialize(val, index, statement);
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(ResultSet resultSet) throws Exception {
        ColumnRowData columnRowData = new ColumnRowData(toInternalConverters.length);

        Row row = resultSet.one();

        for (int i = 0; i < toInternalConverters.length; i++) {
            final Object value = row.getObject(i);
            columnRowData.setField(
                    i, (AbstractBaseColumn) toInternalConverters[i].deserialize(value));
        }
        return columnRowData;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BoundStatement toExternal(RowData rowData, BoundStatement statement) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, statement);
        }
        return statement;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOL":
            case "BOOLEAN":
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case "TINYINT":
                return val -> new ByteColumn((byte) val);
            case "SMALLINT":
                return val -> new BigDecimalColumn((Short) val);
            case "INTEGER":
            case "INT":
                return val -> new BigDecimalColumn((Integer) val);
            case "FLOAT":
                return val -> new BigDecimalColumn((Float) val);
            case "DOUBLE":
                return val -> new BigDecimalColumn((Double) val);
            case "LONG":
            case "BIGINT":
                return val -> new BigDecimalColumn((Long) val);
            case "DECIMAL":
                return val -> new BigDecimalColumn((BigDecimal) val);
            case "VARCHAR":
            case "STRING":
                return val -> new StringColumn((String) val);
            case "TIME":
            case "TIME_WITHOUT_TIME_ZONE":
            case "DATE":
            case "TIMESTAMP":
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
                return val -> new TimestampColumn(DateUtil.getTimestampFromStr(val.toString()));
            case "BINARY":
                return val -> new BytesColumn((byte[]) val);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<BoundStatement> createExternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
            case "BOOL":
                return (val, index, statement) ->
                        statement.setBool(columnNameList.get(index), val.getBoolean(index));
            case "TINYINT":
            case "CHAR":
            case "INT8":
                return (val, index, statement) ->
                        statement.setByte(columnNameList.get(index), val.getByte(index));
            case "INT16":
            case "SMALLINT":
                return (val, index, statement) ->
                        statement.setShort(columnNameList.get(index), val.getShort(index));
            case "INTEGER":
            case "INT":
            case "INT32":
                return (val, index, statement) ->
                        statement.setInt(columnNameList.get(index), val.getInt(index));
            case "BIGINT":
            case "INT64":
                return (val, index, operator) ->
                        operator.setLong(columnNameList.get(index), val.getLong(index));
            case "FLOAT":
                return (val, index, statement) ->
                        statement.setFloat(columnNameList.get(index), val.getFloat(index));
            case "DOUBLE":
                return (val, index, statement) ->
                        statement.setDouble(columnNameList.get(index), val.getDouble(index));
            case "BINARY":
                return (val, index, statement) ->
                        statement.setBytes(
                                columnNameList.get(index), ByteBuffer.wrap(val.getBinary(index)));
            case "DECIMAL":
                return (val, index, statement) ->
                        statement.setDecimal(
                                columnNameList.get(index),
                                ((ColumnRowData) val).getField(index).asBigDecimal());
            case "VARCHAR":
                return (val, index, statement) ->
                        statement.setString(
                                columnNameList.get(index), val.getString(index).toString());
            case "TIME_WITHOUT_TIME_ZONE":
            case "TIME":
                return (val, index, statement) ->
                        statement.setTime(
                                columnNameList.get(index),
                                Time.valueOf(
                                                ((ColumnRowData) val)
                                                        .getField(index)
                                                        .asTimestamp()
                                                        .toLocalDateTime()
                                                        .toLocalTime())
                                        .getTime());
            case "DATE":
                return (val, index, statement) ->
                        statement.setDate(
                                columnNameList.get(index),
                                com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(
                                        ((ColumnRowData) val)
                                                .getField(index)
                                                .asTimestamp()
                                                .toLocalDateTime()
                                                .toLocalDate()
                                                .toEpochDay()));

            case "TIMESTAMP":
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
                return (val, index, statement) ->
                        statement.setTimestamp(
                                columnNameList.get(index),
                                ((ColumnRowData) val).getField(index).asTimestamp());
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
