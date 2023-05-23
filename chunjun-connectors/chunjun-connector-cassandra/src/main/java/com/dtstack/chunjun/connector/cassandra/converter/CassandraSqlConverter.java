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

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.util.List;

public class CassandraSqlConverter
        extends AbstractRowConverter<Row, Row, BoundStatement, LogicalType> {

    private static final long serialVersionUID = 1L;

    private final List<String> columnNameList;

    public CassandraSqlConverter(RowType rowType, List<String> columnNameList) {
        super(rowType);
        this.columnNameList = columnNameList;
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
    }

    @Override
    // 将数据库中的数据类型转化为flink的数据类型 input -> row data
    public RowData toInternal(Row input) throws Exception {
        return deserializeInput(input);
    }

    @Override
    public RowData toInternalLookup(Row input) throws Exception {
        return deserializeInput(input);
    }

    @SuppressWarnings("unchecked")
    private RowData deserializeInput(Row input) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = input.getObject(pos);
            genericRowData.setField(pos, toInternalConverters.get(pos).deserialize(field));
        }
        return genericRowData;
    }

    @Override
    public BoundStatement toExternal(RowData rowData, BoundStatement statement) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters.get(index).serialize(rowData, index, statement);
        }
        return statement;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<BoundStatement> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, statement) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                statement.setToNull(index);
            } else {
                serializationConverter.serialize(val, index, statement);
            }
        };
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case INTEGER:
            case BIGINT:
            case SMALLINT:
                return val -> val;
            case TINYINT:
                return val -> (byte) val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val -> (int) Date.valueOf(String.valueOf(val)).toLocalDate().toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int) (new Time(((Long) val)).toLocalTime().toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromEpochMillis(((java.util.Date) val).getTime());
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> (byte[]) val;
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedTypeException(type);
        }
    }

    @Override
    protected ISerializationConverter<BoundStatement> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) ->
                        statement.setBool(columnNameList.get(index), val.getBoolean(index));
            case TINYINT:
                return (val, index, statement) ->
                        statement.setByte(columnNameList.get(index), val.getByte(index));
            case SMALLINT:
                return (val, index, statement) ->
                        statement.setShort(columnNameList.get(index), val.getShort(index));
            case INTEGER:
                return (val, index, statement) ->
                        statement.setInt(columnNameList.get(index), val.getInt(index));
            case FLOAT:
                return (val, index, statement) ->
                        statement.setFloat(columnNameList.get(index), val.getFloat(index));
            case DOUBLE:
                return (val, index, statement) ->
                        statement.setDouble(columnNameList.get(index), val.getDouble(index));
            case BIGINT:
                return (val, index, statement) ->
                        statement.setLong(columnNameList.get(index), val.getLong(index));
            case CHAR:
            case VARCHAR:
                return (val, index, statement) ->
                        statement.setString(
                                columnNameList.get(index), val.getString(index).toString());
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index, statement) ->
                        statement.setDecimal(
                                columnNameList.get(index),
                                val.getDecimal(index, decimalPrecision, decimalScale)
                                        .toBigDecimal());
            case DATE:
                // TODO date 数据类型存在问题，能够写入，但是数据值不对
                return (val, index, statement) ->
                        statement.setDate(
                                columnNameList.get(index),
                                com.datastax.driver.core.LocalDate.fromMillisSinceEpoch(
                                        val.getInt(index)));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTime(
                                columnNameList.get(index), (val.getInt(index) * 1_000_000L));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index, statement) ->
                        statement.setTimestamp(
                                columnNameList.get(index),
                                val.getTimestamp(index, timestampPrecision).toTimestamp());
            case BINARY:
            case VARBINARY:
                return (val, index, statement) -> {
                    if (String.valueOf(val).length() == 0) {
                        statement.setToNull(columnNameList.get(index));

                    } else {
                        String s = val.toString();
                        byte[] byteArray = new byte[s.length() / 2];
                        for (int i = 0; i < byteArray.length; i++) {
                            String subStr = s.substring(2 * i, 2 * i + 2);
                            byteArray[i] = ((byte) Integer.parseInt(subStr, 16));
                        }
                        statement.setBytes(columnNameList.get(index), ByteBuffer.wrap(byteArray));
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
