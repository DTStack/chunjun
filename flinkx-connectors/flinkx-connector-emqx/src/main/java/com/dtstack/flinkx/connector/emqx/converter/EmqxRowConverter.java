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

package com.dtstack.flinkx.connector.emqx.converter;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.util.JsonUtil;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * @author chuixue
 * @create 2021-06-02 09:34
 * @description
 */
public class EmqxRowConverter extends AbstractRowConverter<String, Object, Object, LogicalType> {

    /** DeserializationSchema Instead of source */
    private DeserializationSchema<RowData> valueDeserialization;

    public EmqxRowConverter(DeserializationSchema<RowData> valueDeserialization) {
        this.valueDeserialization = valueDeserialization;
    }

    public EmqxRowConverter(RowType rowType) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toExternalConverters[i] =
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]);
        }
    }

    @Override
    protected ISerializationConverter wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, rowData) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                GenericRowData genericRowData = (GenericRowData) rowData;
                genericRowData.setField(index, null);
            } else {
                serializationConverter.serialize(val, index, rowData);
            }
        };
    }

    @Override
    public RowData toInternal(String input) throws Exception {
        return valueDeserialization.deserialize(input.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Object toExternal(RowData rowData, Object output) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowData.getArity());
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters[index].serialize(rowData, index, genericRowData);
        }
        return new MqttMessage(JsonUtil.toBytes(genericRowData.toString()));
    }

    @Override
    protected ISerializationConverter<GenericRowData> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, rowData) -> rowData.setField(index, val.getBoolean(index));
            case TINYINT:
                return (val, index, rowData) -> rowData.setField(index, val.getByte(index));
            case SMALLINT:
                return (val, index, rowData) -> rowData.setField(index, val.getShort(index));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, index, rowData) -> rowData.setField(index, val.getInt(index));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, index, rowData) -> rowData.setField(index, val.getLong(index));
            case FLOAT:
                return (val, index, rowData) -> rowData.setField(index, val.getFloat(index));
            case DOUBLE:
                return (val, index, rowData) -> rowData.setField(index, val.getDouble(index));
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (val, index, rowData) ->
                        rowData.setField(index, val.getString(index).toString());
            case BINARY:
            case VARBINARY:
                return (val, index, rowData) -> rowData.setField(index, val.getBinary(index));
            case DATE:
                return (val, index, rowData) ->
                        rowData.setField(
                                index, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, rowData) ->
                        rowData.setField(
                                index,
                                Time.valueOf(
                                        LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index, rowData) ->
                        rowData.setField(
                                index, val.getTimestamp(index, timestampPrecision).toTimestamp());
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index, rowData) ->
                        rowData.setField(
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
