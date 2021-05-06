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
package com.dtstack.flinkx.connector.binlog.converter;

import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.dtstack.flinkx.connector.binlog.listener.BinlogEventRow;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Date: 2021/04/29
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class BinlogRowConverter extends AbstractCDCRowConverter<BinlogEventRow, LogicalType> {
    private final TimestampFormat timestampFormat;

    public BinlogRowConverter(RowType rowType, TimestampFormat timestampFormat) {
        super.fieldNameList = rowType.getFieldNames();
        this.timestampFormat = timestampFormat;
        super.converters = new IDeserializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            super.converters[i] = createInternalConverter(rowType.getTypeAt(i));
        }
    }

    @Override
    public LinkedList<RowData> toInternal(BinlogEventRow binlogEventRow){
        LinkedList<RowData> result = new LinkedList<>();
        CanalEntry.RowChange rowChange = binlogEventRow.getRowChange();
        String eventType = rowChange.getEventType().toString();
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
            Map<Object, Object> beforeMap = Maps.newHashMapWithExpectedSize(beforeColumnsList.size());
            for (CanalEntry.Column column : beforeColumnsList) {
                beforeMap.put(column.getName(), column.getValue());
            }
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            Map<Object, Object> afterMap = Maps.newHashMapWithExpectedSize(afterColumnsList.size());
            for (CanalEntry.Column column : afterColumnsList) {
                afterMap.put(column.getName(), column.getValue());
            }

            switch (eventType){
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
                    RowData updateBefore = createRowDataByConverters(fieldNameList, converters, beforeMap);
                    updateBefore.setRowKind(RowKind.UPDATE_BEFORE);
                    result.add(updateBefore);

                    RowData updateAfter = createRowDataByConverters(fieldNameList, converters, afterMap);
                    updateAfter.setRowKind(RowKind.UPDATE_AFTER);
                    result.add(updateAfter);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported eventType: " + eventType);
            }
        }
        return result;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return val -> Boolean.getBoolean((String) val);
            case TINYINT:
                return val -> Byte.parseByte((String) val);
            case SMALLINT:
                return val -> Short.parseShort((String) val);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return val -> Integer.parseInt((String) val);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return val -> Long.parseLong((String) val);
            case DATE:
                return val -> {
                    LocalDate date = DateTimeFormatter.ISO_LOCAL_DATE.parse((String) val).query(TemporalQueries.localDate());
                    return (int)date.toEpochDay();
                };
            case TIME_WITHOUT_TIME_ZONE:
                return val -> {
                    TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse((String) val);
                    LocalTime localTime = parsedTime.query(TemporalQueries.localTime());
                    return localTime.toSecondOfDay() * 1000;
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    TemporalAccessor parsedTimestamp;
                    switch(this.timestampFormat) {
                        case SQL:
                            parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse((String) val);
                            break;
                        case ISO_8601:
                            parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse((String) val);
                            break;
                        default:
                            throw new TableException(String.format("Unsupported timestamp format '%s'. Validator should have checked that.", this.timestampFormat));
                    }

                    LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
                    LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());
                    return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return val -> {
                    TemporalAccessor parsedTimestampWithLocalZone;
                    switch (timestampFormat) {
                        case SQL:
                            parsedTimestampWithLocalZone =
                                    SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse((String) val);
                            break;
                        case ISO_8601:
                            parsedTimestampWithLocalZone =
                                    ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.parse((String) val);
                            break;
                        default:
                            throw new TableException(
                                    String.format(
                                            "Unsupported timestamp format '%s'. Validator should have checked that.",
                                            timestampFormat));
                    }
                    LocalTime localTime = parsedTimestampWithLocalZone.query(TemporalQueries.localTime());
                    LocalDate localDate = parsedTimestampWithLocalZone.query(TemporalQueries.localDate());

                    return TimestampData.fromInstant(
                            LocalDateTime.of(localDate, localTime).toInstant(ZoneOffset.UTC));
                };
            case FLOAT:
                return val -> Float.parseFloat((String) val);
            case DOUBLE:
                return val -> Double.parseDouble((String) val);
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString((String) val);
            case DECIMAL:
                return val -> {
                    final int precision = ((DecimalType)type).getPrecision();
                    final int scale = ((DecimalType)type).getScale();
                    return DecimalData.fromBigDecimal(new BigDecimal((String) val), precision, scale);
                };
//            case BINARY:
//            case VARBINARY:
//            case ARRAY:
//            case MAP:
//            case MULTISET:
//            case ROW:
//            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }
}
