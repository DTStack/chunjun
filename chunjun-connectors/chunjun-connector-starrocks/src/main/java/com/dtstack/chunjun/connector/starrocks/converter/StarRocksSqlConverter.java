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

package com.dtstack.chunjun.connector.starrocks.converter;

import com.dtstack.chunjun.connector.starrocks.streamload.StarRocksSinkOP;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;

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

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static com.dtstack.chunjun.connector.starrocks.util.StarRocksUtil.addStrForNum;

public class StarRocksSqlConverter
        extends AbstractRowConverter<Object[], Object[], Map<String, Object>, LogicalType> {

    private static final long serialVersionUID = -176225284276566894L;

    private final List<String> columnList;
    public static final String DATETIME_FORMAT_SHORT = "yyyy-MM-dd HH:mm:ss";

    public StarRocksSqlConverter(RowType rowType, List<String> columnList) {
        super(rowType);
        this.columnList = columnList;
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
    public RowData toInternal(Object[] input) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object field = input[pos];
            genericRowData.setField(pos, toInternalConverters.get(pos).deserialize(field));
        }
        return genericRowData;
    }

    @Override
    public RowData toInternalLookup(Object[] input) throws Exception {
        return toInternal(input);
    }

    @Override
    protected ISerializationConverter<Map<String, Object>> wrapIntoNullableExternalConverter(
            ISerializationConverter<Map<String, Object>> ISerializationConverter,
            LogicalType type) {
        return (rowData, index, output) -> {
            if (rowData == null
                    || rowData.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                output.put(columnList.get(index), null);
            } else {
                ISerializationConverter.serialize(rowData, index, output);
            }
        };
    }

    @Override
    public Map<String, Object> toExternal(RowData rowData, Map<String, Object> output)
            throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, output);
        }
        output.put(
                StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.parse(rowData.getRowKind()).ordinal());
        return output;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return val -> (boolean) val;
            case TINYINT:
                return val -> (byte) val;
            case SMALLINT:
                return val -> (short) val;
            case INTEGER:
                return val -> (int) val;
            case BIGINT:
                return val -> (long) val;
            case FLOAT:
                return val -> (float) val;
            case DOUBLE:
                return val -> (double) val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val -> DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString((String) val);
            case DATE:
                return val -> {
                    String value = (String) val;
                    Date date = Date.valueOf(value);
                    return (int) date.toLocalDate().toEpochDay();
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                final int timestampLen =
                        timestampPrecision == 0
                                ? DATETIME_FORMAT_SHORT.length()
                                : DATETIME_FORMAT_SHORT.length() + 1 + timestampPrecision;
                String formatStr =
                        timestampLen == DATETIME_FORMAT_SHORT.length()
                                ? DATETIME_FORMAT_SHORT
                                : addStrForNum(DATETIME_FORMAT_SHORT + ".", timestampLen, "S");
                return val -> {
                    DateTimeFormatter df = DateTimeFormatter.ofPattern(formatStr);
                    String value = (String) val;
                    if (value.length() < DATETIME_FORMAT_SHORT.length()) {
                        throw new IllegalArgumentException(
                                "Date value length shorter than DATETIME_FORMAT_SHORT");
                    }
                    if (value.length() == timestampLen) {
                        return TimestampData.fromLocalDateTime(LocalDateTime.parse(value, df));
                    } else if (value.length() > timestampLen) {
                        return TimestampData.fromLocalDateTime(
                                LocalDateTime.parse(value.substring(0, timestampLen), df));
                    } else {
                        if (value.length() == DATETIME_FORMAT_SHORT.length()) {
                            return LocalDateTime.parse(
                                    addStrForNum(value + ".", timestampLen, "0"));
                        } else {
                            return LocalDateTime.parse(addStrForNum(value, timestampLen, "0"));
                        }
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<Map<String, Object>> createExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getBoolean(index) ? 1 : 0);
            case TINYINT:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getByte(index));
            case SMALLINT:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getShort(index));
            case INTEGER:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getInt(index));
            case BIGINT:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getLong(index));
            case FLOAT:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getFloat(index));
            case DOUBLE:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getDouble(index));
            case DECIMAL: // for both largeint and decimal
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (rowData, index, map) ->
                        map.put(
                                columnList.get(index),
                                rowData.getDecimal(index, decimalPrecision, decimalScale)
                                        .toBigDecimal());
            case CHAR:
            case VARCHAR:
                return (rowData, index, map) ->
                        map.put(columnList.get(index), rowData.getString(index).toString());
            case DATE:
                return (rowData, index, map) ->
                        map.put(
                                columnList.get(index),
                                Date.valueOf(LocalDate.ofEpochDay(rowData.getInt(index)))
                                        .toString());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (rowData, index, map) ->
                        map.put(
                                columnList.get(index),
                                rowData.getTimestamp(index, timestampPrecision)
                                        .toLocalDateTime()
                                        .toString());
            case BINARY:
                return (rowData, index, map) -> {
                    byte[] bts = rowData.getBinary(index);
                    long value = 0;
                    for (int i = 0; i < bts.length; i++) {
                        value += (bts[bts.length - i - 1] & 0xffL) << (8 * i);
                    }
                    map.put(columnList.get(index), value);
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
