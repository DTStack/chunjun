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
package com.dtstack.chunjun.connector.oraclelogminer.converter;

import com.dtstack.chunjun.connector.oraclelogminer.entity.EventRow;
import com.dtstack.chunjun.connector.oraclelogminer.entity.EventRowData;
import com.dtstack.chunjun.connector.oraclelogminer.listener.LogParser;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.util.DateUtil;

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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class LogMinerRawTypeMapper extends AbstractCDCRawTypeMapper<EventRow, LogicalType> {

    private static final long serialVersionUID = -7611385227503313499L;

    public LogMinerRawTypeMapper(RowType rowType) {
        super.fieldNameList = rowType.getFieldNames();
        super.converters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            super.converters.add(createInternalConverter(rowType.getTypeAt(i)));
        }
    }

    @Override
    public LinkedList<RowData> toInternal(EventRow eventRow) throws Exception {
        LinkedList<RowData> result = new LinkedList<>();

        String eventType = eventRow.getType();

        List<EventRowData> beforeRowDataList = eventRow.getBeforeColumnList();
        Map<Object, Object> beforeMap = Maps.newHashMapWithExpectedSize(beforeRowDataList.size());
        beforeRowDataList.forEach(
                x -> {
                    beforeMap.put(x.getName(), x.getData());
                });

        List<EventRowData> afterRowDataList = eventRow.getAfterColumnList();
        Map<Object, Object> afterMap = Maps.newHashMapWithExpectedSize(afterRowDataList.size());
        afterRowDataList.forEach(x -> afterMap.put(x.getName(), x.getData()));

        switch (eventType) {
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
                return (IDeserializationConverter<String, Boolean>) Boolean::getBoolean;
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
                            val = LogParser.parseTime(val);
                            LocalDate date =
                                    SQL_TIMESTAMP_FORMAT
                                            .parse(val)
                                            .query(TemporalQueries.localDate());
                            return (int) date.toEpochDay();
                        };
            case TIME_WITHOUT_TIME_ZONE:
                return (IDeserializationConverter<String, Integer>)
                        val -> {
                            val = LogParser.parseTime(val);
                            TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(val);
                            LocalTime localTime = parsedTime.query(TemporalQueries.localTime());
                            return localTime.toSecondOfDay() * 1000;
                        };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (IDeserializationConverter<String, TimestampData>)
                        val -> {
                            val = LogParser.parseTime(val);
                            TemporalAccessor parse = DateUtil.DATETIME_FORMATTER.parse(val);
                            LocalTime localTime = parse.query(TemporalQueries.localTime());
                            LocalDate localDate = parse.query(TemporalQueries.localDate());
                            return TimestampData.fromLocalDateTime(
                                    LocalDateTime.of(localDate, localTime));
                        };
            case FLOAT:
                return (IDeserializationConverter<String, Float>) Float::parseFloat;
            case DOUBLE:
                return (IDeserializationConverter<String, Double>) Double::parseDouble;
            case CHAR:
            case VARCHAR:
                return (IDeserializationConverter<String, StringData>)
                        val -> {
                            val = LogParser.parseString(val);
                            return StringData.fromString(val);
                        };
            case DECIMAL:
                return (IDeserializationConverter<String, DecimalData>)
                        val -> {
                            final int precision = ((DecimalType) type).getPrecision();
                            final int scale = ((DecimalType) type).getScale();
                            return DecimalData.fromBigDecimal(
                                    new BigDecimal(val), precision, scale);
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
