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

package com.dtstack.chunjun.connector.nebula.converter;

import com.dtstack.chunjun.connector.nebula.row.NebulaRows;
import com.dtstack.chunjun.connector.nebula.row.NebulaTableRow;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import com.vesoft.nebula.client.graph.data.DateTimeWrapper;
import com.vesoft.nebula.client.graph.data.TimeWrapper;
import com.vesoft.nebula.client.storage.data.BaseTableRow;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

public class NebulaSqlConverter
        extends AbstractRowConverter<BaseTableRow, BaseTableRow, NebulaRows, RowType.RowField> {

    private static final long serialVersionUID = -2353660561978026681L;

    public NebulaSqlConverter(RowType rowType) {
        super(rowType);
        List<RowType.RowField> fields = rowType.getFields();
        for (RowType.RowField field : fields) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(createInternalConverter(field)));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(createExternalConverter(field), field));
        }
    }

    @Override
    public RowData toInternal(BaseTableRow record) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int i = 0; i < toInternalConverters.size(); i++) {
            Object obj = NebulaTableRow.getValue(record.getValues().get(i));
            genericRowData.setField(i, toInternalConverters.get(i).deserialize(obj));
        }
        return genericRowData;
    }

    @Override
    public RowData toInternalLookup(BaseTableRow input) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int i = 0; i < toInternalConverters.size(); i++) {
            Object obj = NebulaTableRow.getValue(input.getValues().get(i));
            genericRowData.setField(i, toInternalConverters.get(i).deserialize(obj));
        }
        return genericRowData;
    }

    @Override
    public NebulaRows toExternal(RowData rowData, NebulaRows output) throws Exception {
        for (int i = 0; i < toExternalConverters.size(); i++) {
            toExternalConverters.get(i).serialize(rowData, i, output);
        }
        output.build();
        return output;
    }

    @Override
    protected ISerializationConverter<NebulaRows> wrapIntoNullableExternalConverter(
            ISerializationConverter<NebulaRows> serializationConverter, RowType.RowField type) {
        return (val, index, rows) -> {
            if (val == null) {
                rows.getValues().add(index, null);
            } else {
                serializationConverter.serialize(val, index, rows);
            }
        };
    }

    @Override
    protected IDeserializationConverter createInternalConverter(RowType.RowField rowField) {
        LogicalType type = rowField.getType();
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return val -> val;
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case DATE:
                return val -> {
                    Date date = Date.valueOf(val.toString());
                    return (int) date.toLocalDate().toEpochDay();
                };
            case TIME_WITHOUT_TIME_ZONE:
                return val -> {
                    TimeWrapper t = (TimeWrapper) val;
                    LocalTime localTime = LocalTime.of(t.getHour(), t.getMinute(), t.getSecond());
                    Time time = Time.valueOf(localTime);
                    return (int) (time.toLocalTime().toNanoOfDay() / 1_000_000L);
                };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return val -> {
                    if (val instanceof DateTimeWrapper) {
                        DateTimeWrapper t = (DateTimeWrapper) val;
                        LocalDateTime localDateTime =
                                LocalDateTime.of(
                                        t.getYear(),
                                        t.getMonth(),
                                        t.getDay(),
                                        t.getHour(),
                                        t.getMinute(),
                                        t.getSecond());
                        return TimestampData.fromLocalDateTime(localDateTime);
                    } else {
                        return TimestampData.fromTimestamp(new Timestamp((long) val * 1000));
                    }
                };
            case BINARY:
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case VARBINARY:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<NebulaRows> createExternalConverter(
            RowType.RowField rowField) {
        LogicalType type = rowField.getType();
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, rows) ->
                        rows.getValues().add(index, String.valueOf(val.getBoolean(index)));
            case TINYINT:
                return (val, index, rows) ->
                        rows.getValues().add(index, String.valueOf(val.getByte(index)));
            case SMALLINT:
                return (val, index, rows) ->
                        rows.getValues().add(index, String.valueOf(val.getShort(index)));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, index, rows) ->
                        rows.getValues().add(index, String.valueOf(val.getInt(index)));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, index, rows) ->
                        rows.getValues().add(index, String.valueOf(val.getLong(index)));
            case FLOAT:
                return (val, index, rows) ->
                        rows.getValues().add(index, String.valueOf(val.getFloat(index)));
            case DOUBLE:
                return (val, index, rows) ->
                        rows.getValues().add(index, String.valueOf(val.getDouble(index)));
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (val, index, rows) ->
                        rows.getValues().add(index, "\"" + val.getString(index).toString() + "\"");
            case BINARY:
            case VARBINARY:
                return (val, index, rows) ->
                        rows.getValues().add(index, "\"" + new String(val.getBinary(index)) + "\"");
            case DATE:
                return (val, index, rows) ->
                        rows.getValues()
                                .add(
                                        index,
                                        "date(\""
                                                + Date.valueOf(
                                                        LocalDate.ofEpochDay(val.getInt(index)))
                                                + "\")");
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, rows) ->
                        rows.getValues()
                                .add(
                                        index,
                                        "time(\""
                                                + Time.valueOf(
                                                        LocalTime.ofNanoOfDay(
                                                                val.getInt(index) * 1_000_000L))
                                                + "\")");
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index, rows) ->
                        rows.getValues()
                                .add(
                                        index,
                                        String.valueOf(
                                                val.getTimestamp(index, timestampPrecision)
                                                                .toTimestamp()
                                                                .getTime()
                                                        / 1000));
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index, rows) ->
                        rows.getValues()
                                .add(
                                        index,
                                        String.valueOf(
                                                val.getDecimal(
                                                                index,
                                                                decimalPrecision,
                                                                decimalScale)
                                                        .toUnscaledLong()));
            case ROW:
            case ARRAY:
            case MULTISET:
            case MAP:
            case STRUCTURED_TYPE:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
