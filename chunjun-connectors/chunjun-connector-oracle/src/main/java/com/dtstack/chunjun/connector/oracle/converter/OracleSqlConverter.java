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

import com.dtstack.chunjun.connector.jdbc.converter.JdbcSqlConverter;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import io.vertx.core.json.JsonArray;
import oracle.sql.TIMESTAMP;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.ArrayList;

public class OracleSqlConverter extends JdbcSqlConverter {

    private static final long serialVersionUID = 1L;

    public static final int CLOB_LENGTH = 4000;

    protected ArrayList<IDeserializationConverter> toAsyncInternalConverters;

    public OracleSqlConverter(RowType rowType) {
        super(rowType);
        toAsyncInternalConverters = new ArrayList<>(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toAsyncInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createAsyncInternalConverter(rowType.getTypeAt(i))));
        }
        toInternalConverters = toAsyncInternalConverters;
    }

    protected IDeserializationConverter createAsyncInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BINARY:
            case VARBINARY:
                return val -> val;
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            default:
                return createInternalConverter(type);
        }
    }

    @Override
    public RowData toInternalLookup(JsonArray jsonArray) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = jsonArray.getValue(pos);
            genericRowData.setField(pos, toAsyncInternalConverters.get(pos).deserialize(field));
        }
        return genericRowData;
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
                return val -> val;
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case BIGINT:
                return val -> Long.valueOf(val.toString());
            case INTEGER:
                return val -> Integer.valueOf(val.toString());
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val ->
                        val instanceof Timestamp
                                ? (int)
                                        (((Timestamp) val)
                                                .toLocalDateTime()
                                                .toLocalDate()
                                                .toEpochDay())
                                : (int)
                                        ((Date.valueOf(String.valueOf(val)))
                                                .toLocalDate()
                                                .toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int)
                                ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    if (val instanceof String) {
                        // oracle.sql.TIMESTAMP will return cast to String in lookup
                        return TimestampData.fromTimestamp(Timestamp.valueOf((String) val));
                    } else if (val instanceof TIMESTAMP) {
                        try {
                            return TimestampData.fromTimestamp(((TIMESTAMP) val).timestampValue());
                        } catch (SQLException e) {
                            throw new UnsupportedTypeException(
                                    "Unsupported type:" + type + ",value:" + val);
                        }
                    } else {
                        return TimestampData.fromTimestamp(((Timestamp) val));
                    }
                };
            case CHAR:
                if (((CharType) type).getLength() > CLOB_LENGTH
                        && ((VarCharType) type).getLength() != Integer.MAX_VALUE) {
                    return val -> {
                        oracle.sql.CLOB clob = (oracle.sql.CLOB) val;
                        return StringData.fromString(ConvertUtil.convertClob(clob));
                    };
                }
                return val -> StringData.fromString(val.toString());
            case VARCHAR:
                if (((VarCharType) type).getLength() > CLOB_LENGTH
                        && ((VarCharType) type).getLength() != Integer.MAX_VALUE) {
                    return val -> {
                        oracle.sql.CLOB clob = (oracle.sql.CLOB) val;
                        return StringData.fromString(ConvertUtil.convertClob(clob));
                    };
                }
                return val -> StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> {
                    oracle.sql.BLOB blob = (oracle.sql.BLOB) val;
                    return ConvertUtil.toByteArray(blob);
                };
            default:
                throw new UnsupportedTypeException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) ->
                        statement.setBoolean(index, val.getBoolean(index));
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index, val.getByte(index));
            case SMALLINT:
                return (val, index, statement) -> statement.setShort(index, val.getShort(index));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, index, statement) -> statement.setInt(index, val.getInt(index));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, index, statement) -> statement.setLong(index, val.getLong(index));
            case FLOAT:
                return (val, index, statement) -> statement.setFloat(index, val.getFloat(index));
            case DOUBLE:
                return (val, index, statement) -> statement.setDouble(index, val.getDouble(index));
            case CHAR:
                if (((CharType) type).getLength() > CLOB_LENGTH
                        && ((VarCharType) type).getLength() != Integer.MAX_VALUE) {
                    return (val, index, statement) -> {
                        try (StringReader reader =
                                new StringReader(val.getString(index).toString())) {
                            statement.setClob(index, reader);
                        }
                    };
                }
                return (val, index, statement) -> {
                    statement.setString(index, val.getString(index).toString());
                };
            case VARCHAR:
                // value is BinaryString
                if (((VarCharType) type).getLength() > CLOB_LENGTH
                        && ((VarCharType) type).getLength() != Integer.MAX_VALUE) {
                    return (val, index, statement) -> {
                        try (StringReader reader =
                                new StringReader(val.getString(index).toString())) {
                            statement.setClob(index, reader);
                        }
                    };
                }
                return (val, index, statement) -> {
                    statement.setString(index, val.getString(index).toString());
                };
            case BINARY:
            case VARBINARY:
                return (val, index, statement) -> {
                    try (InputStream is = new ByteArrayInputStream(val.getBinary(index))) {
                        statement.setBlob(index, is);
                    }
                };
            case DATE:
                return (val, index, statement) ->
                        statement.setDate(
                                index,
                                new Date(new Timestamp(val.getInt(index) * 1000L).getTime()));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTime(
                                index,
                                Time.valueOf(
                                        LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, val.getTimestamp(index, timestampPrecision).toTimestamp());
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index, statement) ->
                        statement.setBigDecimal(
                                index,
                                val.getDecimal(index, decimalPrecision, decimalScale)
                                        .toBigDecimal());
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
