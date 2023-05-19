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

package com.dtstack.chunjun.connector.oceanbasecdc.converter;

import com.dtstack.chunjun.connector.oceanbasecdc.entity.OceanBaseCdcEventRow;
import com.dtstack.chunjun.connector.oceanbasecdc.format.TimestampFormat;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.converter.IDeserializationConverter;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.oceanbase.oms.logmessage.DataMessage;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

@SuppressWarnings("rawtypes, unchecked")
public class OceanBaseCdcSqlConverter
        extends AbstractCDCRawTypeMapper<OceanBaseCdcEventRow, LogicalType> {
    private static final long serialVersionUID = -7787763548679643153L;
    private final TimestampFormat timestampFormat;

    public OceanBaseCdcSqlConverter(RowType rowType, TimestampFormat timestampFormat) {
        super.fieldNameList = rowType.getFieldNames();
        this.timestampFormat = timestampFormat;
        super.converters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            super.converters.add(createInternalConverter(rowType.getTypeAt(i)));
        }
    }

    @Override
    public LinkedList<RowData> toInternal(OceanBaseCdcEventRow eventRow) throws Exception {
        LinkedList<RowData> result = new LinkedList<>();
        DataMessage.Record.Type eventType = eventRow.getType();
        switch (eventType) {
            case INSERT:
                RowData insert =
                        createRowDataByConverters(
                                fieldNameList,
                                converters,
                                OceanBaseCdcEventRow.toValueMap(eventRow.getFieldsAfter()));
                insert.setRowKind(RowKind.INSERT);
                result.add(insert);
                break;
            case DELETE:
                RowData delete =
                        createRowDataByConverters(
                                fieldNameList,
                                converters,
                                OceanBaseCdcEventRow.toValueMap(eventRow.getFieldsBefore()));
                delete.setRowKind(RowKind.DELETE);
                result.add(delete);
                break;
            case UPDATE:
                RowData updateBefore =
                        createRowDataByConverters(
                                fieldNameList,
                                converters,
                                OceanBaseCdcEventRow.toValueMap(eventRow.getFieldsBefore()));
                updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
                result.add(updateBefore);

                RowData updateAfter =
                        createRowDataByConverters(
                                fieldNameList,
                                converters,
                                OceanBaseCdcEventRow.toValueMap(eventRow.getFieldsAfter()));
                updateAfter.setRowKind(RowKind.UPDATE_AFTER);
                result.add(updateAfter);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported eventType: " + eventType);
        }
        return result;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return (IDeserializationConverter<String, Boolean>) "1"::equals;
            case TINYINT:
                return (IDeserializationConverter<String, Byte>) Byte::parseByte;
            case SMALLINT:
                return (IDeserializationConverter<String, Short>) Short::parseShort;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (IDeserializationConverter<String, Integer>) Integer::parseInt;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (IDeserializationConverter<String, Long>) Long::parseLong;
            case DATE:
                return (IDeserializationConverter<String, Integer>)
                        val -> {
                            LocalDate date =
                                    DateTimeFormatter.ISO_LOCAL_DATE
                                            .parse(val)
                                            .query(TemporalQueries.localDate());
                            return (int) date.toEpochDay();
                        };
            case TIME_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<String, Integer>)
                        val -> {
                            TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(val);
                            LocalTime localTime = parsedTime.query(TemporalQueries.localTime());
                            return localTime.toSecondOfDay() * 1000;
                        };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<String, TimestampData>)
                        val -> {
                            TemporalAccessor parsedTimestamp;
                            switch (this.timestampFormat) {
                                case SQL:
                                    parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(val);
                                    break;
                                case ISO_8601:
                                    parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(val);
                                    break;
                                default:
                                    throw new TableException(
                                            String.format(
                                                    "Unsupported timestamp format '%s'. Validator should have checked that.",
                                                    this.timestampFormat));
                            }

                            LocalTime localTime =
                                    parsedTimestamp.query(TemporalQueries.localTime());
                            LocalDate localDate =
                                    parsedTimestamp.query(TemporalQueries.localDate());
                            return TimestampData.fromLocalDateTime(
                                    LocalDateTime.of(localDate, localTime));
                        };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (IDeserializationConverter<String, TimestampData>)
                        val -> {
                            TemporalAccessor parsedTimestampWithLocalZone;
                            switch (timestampFormat) {
                                case SQL:
                                    parsedTimestampWithLocalZone =
                                            SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(val);
                                    break;
                                case ISO_8601:
                                    parsedTimestampWithLocalZone =
                                            ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse(val);
                                    break;
                                default:
                                    throw new TableException(
                                            String.format(
                                                    "Unsupported timestamp format '%s'. Validator should have checked that.",
                                                    timestampFormat));
                            }
                            LocalTime localTime =
                                    parsedTimestampWithLocalZone.query(TemporalQueries.localTime());
                            LocalDate localDate =
                                    parsedTimestampWithLocalZone.query(TemporalQueries.localDate());

                            return TimestampData.fromInstant(
                                    LocalDateTime.of(localDate, localTime)
                                            .toInstant(ZoneOffset.UTC));
                        };
            case FLOAT:
                return (IDeserializationConverter<String, Float>) Float::parseFloat;
            case DOUBLE:
                return (IDeserializationConverter<String, Double>) Double::parseDouble;
            case CHAR:
            case VARCHAR:
                return (IDeserializationConverter<String, StringData>) StringData::fromString;
            case DECIMAL:
                return (IDeserializationConverter<String, DecimalData>)
                        val -> {
                            final int precision = ((DecimalType) type).getPrecision();
                            final int scale = ((DecimalType) type).getScale();
                            return DecimalData.fromBigDecimal(
                                    new BigDecimal(val), precision, scale);
                        };
            case BINARY:
                return (IDeserializationConverter<String, byte[]>)
                        val -> {
                            long v = Long.parseLong(val);
                            byte[] bytes = ByteBuffer.allocate(8).putLong(v).array();
                            int i = 0;
                            while (i < Long.BYTES - 1 && bytes[i] == 0) {
                                i++;
                            }
                            return Arrays.copyOfRange(bytes, i, Long.BYTES);
                        };
            case VARBINARY:
                return (IDeserializationConverter<String, byte[]>)
                        val -> val.getBytes(StandardCharsets.UTF_8);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
