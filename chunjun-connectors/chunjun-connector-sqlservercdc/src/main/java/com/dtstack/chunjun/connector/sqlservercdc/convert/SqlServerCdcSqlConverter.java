/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.sqlservercdc.convert;

import com.dtstack.chunjun.connector.sqlservercdc.entity.ChangeTable;
import com.dtstack.chunjun.connector.sqlservercdc.entity.SqlServerCdcEventRow;
import com.dtstack.chunjun.connector.sqlservercdc.format.TimestampFormat;
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

import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class SqlServerCdcSqlConverter
        extends AbstractCDCRawTypeMapper<SqlServerCdcEventRow, LogicalType> {
    private static final long serialVersionUID = -7462105746858831513L;
    private final TimestampFormat timestampFormat;

    public SqlServerCdcSqlConverter(RowType rowType, TimestampFormat timestampFormat) {
        super.fieldNameList = rowType.getFieldNames();
        this.timestampFormat = timestampFormat;
        super.converters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            super.converters.add(createInternalConverter(rowType.getTypeAt(i)));
        }
    }

    @Override
    public LinkedList<RowData> toInternal(SqlServerCdcEventRow sqlServerCdcEventRow)
            throws Exception {
        LinkedList<RowData> result = new LinkedList<>();
        ChangeTable changeTable = sqlServerCdcEventRow.getChangeTable();
        String eventType = sqlServerCdcEventRow.getType();
        Object[] dataPrev = sqlServerCdcEventRow.getDataPrev();
        Object[] data = sqlServerCdcEventRow.getData();
        List<String> columnList = changeTable.getColumnList();

        Map<Object, Object> beforeMap = Maps.newHashMapWithExpectedSize(dataPrev.length);
        for (int columnIndex = 0; columnIndex < dataPrev.length; columnIndex++) {
            beforeMap.put(columnList.get(columnIndex), dataPrev[columnIndex]);
        }
        Map<Object, Object> afterMap = Maps.newHashMapWithExpectedSize(data.length);
        for (int columnIndex = 0; columnIndex < data.length; columnIndex++) {
            afterMap.put(columnList.get(columnIndex), data[columnIndex]);
        }
        switch (eventType.toUpperCase(Locale.ENGLISH)) {
            case "INSERT":
                RowData insert = createRowDataByConverters(fieldNameList, converters, afterMap);
                insert.setRowKind(RowKind.INSERT);
                result.add(insert);
                break;
            case "DELETE":
                RowData delete = createRowDataByConverters(fieldNameList, converters, beforeMap);
                delete.setRowKind(RowKind.DELETE);
                result.add(delete);
                break;
            case "UPDATE":
                RowData updateBefore =
                        createRowDataByConverters(fieldNameList, converters, beforeMap);
                updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
                result.add(updateBefore);

                RowData updateAfter =
                        createRowDataByConverters(fieldNameList, converters, afterMap);
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
                return (IDeserializationConverter<Boolean, Boolean>) Boolean::valueOf;
            case TINYINT:
            case SMALLINT:
                return (IDeserializationConverter<Short, Byte>) Short::byteValue;
            case INTEGER:
                return (IDeserializationConverter<Integer, Integer>) Integer::valueOf;
            case INTERVAL_YEAR_MONTH:
                return (IDeserializationConverter<String, Integer>) Integer::parseInt;
            case BIGINT:
                return (IDeserializationConverter<Long, Long>) Long::valueOf;
            case INTERVAL_DAY_TIME:
                return (IDeserializationConverter<String, Long>) Long::parseLong;
            case DATE:
                return (IDeserializationConverter<Date, Integer>)
                        val -> (int) val.toLocalDate().toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Time, Integer>)
                        val -> val.toLocalTime().toSecondOfDay() * 1000;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<Timestamp, TimestampData>)
                        TimestampData::fromTimestamp;
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
            case DOUBLE:
                return (IDeserializationConverter<Double, Double>) Double::valueOf;
            case FLOAT:
                return (IDeserializationConverter<Float, Float>) Float::valueOf;
            case CHAR:
            case VARCHAR:
                return (IDeserializationConverter<String, StringData>) StringData::fromString;
            case DECIMAL:
                return (IDeserializationConverter<BigDecimal, DecimalData>)
                        val -> {
                            final int precision = ((DecimalType) type).getPrecision();
                            final int scale = ((DecimalType) type).getScale();
                            return DecimalData.fromBigDecimal(val, precision, scale);
                        };
            case BINARY:
            case VARBINARY:
                return (IDeserializationConverter<String, byte[]>)
                        val -> val.getBytes(StandardCharsets.UTF_8);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
