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

package com.dtstack.chunjun.connector.cassandra.converter;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;
import com.dtstack.chunjun.util.DateUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class CassandraSyncConverter
        extends AbstractRowConverter<ResultSet, Row, BoundStatement, String> {

    private static final long serialVersionUID = 9079536637671380594L;

    private final List<FieldConfig> fieldConfList;

    public CassandraSyncConverter(RowType rowType, List<FieldConfig> fieldConfList) {
        super(rowType);
        this.fieldConfList = fieldConfList;
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(fieldConfList.get(i).getType().getType())));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldConfList.get(i).getType().getType()),
                            fieldConfList.get(i).getType().getType()));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public ISerializationConverter<BoundStatement> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, String type) {
        return (val, index, statement) -> {
            if (val == null || val.isNullAt(index)) {
                statement.setToNull(index);
            } else {
                serializationConverter.serialize(val, index, statement);
            }
        };
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(ResultSet resultSet) throws Exception {
        ColumnRowData columnRowData = new ColumnRowData(toInternalConverters.size());

        Row row = resultSet.one();

        for (int i = 0; i < toInternalConverters.size(); i++) {
            final Object value = row.getObject(i);
            columnRowData.setField(
                    i, (AbstractBaseColumn) toInternalConverters.get(i).deserialize(value));
        }
        return columnRowData;
    }

    @Override
    @SuppressWarnings("unchecked")
    public BoundStatement toExternal(RowData rowData, BoundStatement statement) throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, statement);
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
                return val -> new ShortColumn((Short) val);
            case "VARINT":
                return val -> {
                    if (val instanceof BigDecimal) {
                        return new BigDecimalColumn((BigDecimal) val);
                    }

                    if (val instanceof BigInteger) {
                        return new BigDecimalColumn((BigInteger) val);
                    }
                    return new BigDecimalColumn((Short) val);
                };
            case "INTEGER":
            case "INT":
                return val -> new IntColumn((Integer) val);
            case "FLOAT":
                return val -> new FloatColumn((Float) val);
            case "DOUBLE":
                return val -> new DoubleColumn((Double) val);
            case "LONG":
            case "BIGINT":
            case "COUNTER":
                return val -> new LongColumn((Long) val);
            case "DECIMAL":
                return val -> new BigDecimalColumn((BigDecimal) val);
            case "VARCHAR":
            case "STRING":
            case "TEXT":
            case "ASCII":
            case "INET":
            case "UUID":
            case "TIMEUUID":
                return val -> new StringColumn((String) val);
            case "TIME":
            case "TIME_WITHOUT_TIME_ZONE":
                return val -> new TimeColumn(new Time((Long) val));
            case "DATE":
                return val -> new SqlDateColumn(Date.valueOf(val.toString()));
            case "TIMESTAMP":
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
                return val -> {
                    if (val instanceof Date) {
                        new TimestampColumn(
                                DateUtil.getTimestampFromStr(
                                        ((Date) val).toLocalDate().toString()));
                    }

                    if (val instanceof java.util.Date) {
                        return new TimestampColumn(new Timestamp(((java.util.Date) val).getTime()));
                    }
                    return new TimestampColumn(DateUtil.getTimestampFromStr(val.toString()));
                };
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return val -> {
                    if (val instanceof ByteBuffer) {
                        return new BytesColumn(((ByteBuffer) val).array());
                    }

                    return new BytesColumn((byte[]) val);
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<BoundStatement> createExternalConverter(String type) {
        switch (type.toUpperCase(Locale.ENGLISH)) {
            case "INET":
                return (val, index, statement) ->
                        statement.setInet(
                                fieldConfList.get(index).getName(),
                                InetAddress.getByName(val.getString(index).toString()));
            case "UUID":
            case "TIMEUUID":
                return (val, index, statement) ->
                        statement.setUUID(
                                fieldConfList.get(index).getName(),
                                UUID.fromString(val.getString(index).toString()));
            case "BOOLEAN":
            case "BOOL":
                return (val, index, statement) ->
                        statement.setBool(
                                fieldConfList.get(index).getName(), val.getBoolean(index));
            case "TINYINT":
            case "CHAR":
                return (val, index, statement) ->
                        statement.setByte(fieldConfList.get(index).getName(), val.getByte(index));
            case "SMALLINT":
            case "VARINT":
                return (val, index, statement) ->
                        statement.setShort(fieldConfList.get(index).getName(), val.getShort(index));
            case "INTEGER":
            case "INT":
                return (val, index, statement) ->
                        statement.setInt(fieldConfList.get(index).getName(), val.getInt(index));
            case "BIGINT":
                return (val, index, operator) ->
                        operator.setLong(fieldConfList.get(index).getName(), val.getLong(index));
            case "FLOAT":
                return (val, index, statement) ->
                        statement.setFloat(fieldConfList.get(index).getName(), val.getFloat(index));
            case "DOUBLE":
                return (val, index, statement) ->
                        statement.setDouble(
                                fieldConfList.get(index).getName(), val.getDouble(index));
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
                return (val, index, statement) ->
                        statement.setBytes(
                                fieldConfList.get(index).getName(),
                                ByteBuffer.wrap(val.getBinary(index)));
            case "DECIMAL":
                return (val, index, statement) ->
                        statement.setDecimal(
                                fieldConfList.get(index).getName(),
                                ((ColumnRowData) val).getField(index).asBigDecimal());
            case "VARCHAR":
            case "ASCII":
            case "STRING":
            case "TEXT":
                return (val, index, statement) ->
                        statement.setString(
                                fieldConfList.get(index).getName(),
                                val.getString(index).toString());
            case "TIME_WITHOUT_TIME_ZONE":
            case "TIME":
                return (val, index, statement) ->
                        statement.setTime(
                                fieldConfList.get(index).getName(),
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
                                fieldConfList.get(index).getName(),
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
                                fieldConfList.get(index).getName(),
                                ((ColumnRowData) val).getField(index).asTimestamp());
            default:
                throw new UnsupportedTypeException(type);
        }
    }
}
