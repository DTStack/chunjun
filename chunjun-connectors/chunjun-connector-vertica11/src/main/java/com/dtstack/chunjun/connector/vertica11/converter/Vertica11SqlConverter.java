/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.vertica11.converter;

import com.dtstack.chunjun.connector.jdbc.converter.JdbcSqlConverter;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.util.DateUtil;

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

import io.vertx.core.json.JsonArray;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;

public class Vertica11SqlConverter extends JdbcSqlConverter {

    private static final long serialVersionUID = -4636169894477054710L;

    protected ArrayList<IDeserializationConverter> toAsyncInternalConverters;

    public Vertica11SqlConverter(RowType rowType) {
        super(rowType);
        toAsyncInternalConverters = new ArrayList<>(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toAsyncInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createAsyncInternalConverter(rowType.getTypeAt(i))));
        }
    }

    protected IDeserializationConverter createAsyncInternalConverter(LogicalType type) {
        if (type.getTypeRoot() == LogicalTypeRoot.INTEGER) {
            return (IDeserializationConverter<Long, Long>) val -> new Long(val.toString());
        }
        return createInternalConverter(type);
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

    /**
     * Convert external database type to flink internal type.
     *
     * @param type
     * @return
     */
    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {

        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case BIGINT:
            case CHAR:
            case BINARY:
            case VARBINARY:
                return (IDeserializationConverter<Boolean, Boolean>) val -> val;
            case FLOAT:
            case TINYINT:
            case SMALLINT:
            case DECIMAL:
                return (IDeserializationConverter<BigDecimal, DecimalData>)
                        val -> DecimalData.fromBigDecimal(val, val.precision(), val.scale());
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case INTEGER:
                return (IDeserializationConverter<Long, Long>) val -> new Long(val.toString());
            case DATE:
                return (IDeserializationConverter<String, Integer>)
                        val ->
                                (int)
                                        DateUtil.getTimestampFromStr(val)
                                                .toLocalDateTime()
                                                .toLocalDate()
                                                .toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<String, Integer>)
                        val ->
                                (int)
                                        ((Time.valueOf(String.valueOf(val)))
                                                        .toLocalTime()
                                                        .toNanoOfDay()
                                                / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp((Timestamp) val);
            case VARCHAR:
                return (IDeserializationConverter<String, StringData>) StringData::fromString;
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    /**
     * Convert data types inside flink to external database system types.
     *
     * @param type
     * @return
     */
    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) ->
                        statement.setBoolean(index, val.getBoolean(index));
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return (val, index, statement) ->
                        statement.setLong(index, new Long((val.getInt(index))));
            case INTERVAL_YEAR_MONTH:
            case BIGINT:
                return (val, index, statement) -> statement.setLong(index, val.getLong(index));
            case INTERVAL_DAY_TIME:
            case FLOAT:
                return (val, index, statement) -> statement.setFloat(index, val.getFloat(index));
            case DOUBLE:
                return (val, index, statement) -> statement.setDouble(index, val.getDouble(index));
            case CHAR:
            case VARCHAR:
                return (val, index, statement) ->
                        statement.setString(index, val.getString(index).toString());
            case BINARY:
            case VARBINARY:
                return (val, index, statement) -> statement.setBytes(index, val.getBinary(index));
            case DATE:
                return (val, index, statement) ->
                        statement.setDate(
                                index, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
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
