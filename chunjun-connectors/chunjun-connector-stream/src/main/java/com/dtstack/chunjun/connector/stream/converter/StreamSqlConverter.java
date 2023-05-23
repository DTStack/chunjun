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

package com.dtstack.chunjun.connector.stream.converter;

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.util.ExternalDataUtil;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import com.github.jsonzou.jmockdata.JMockData;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static java.time.temporal.ChronoField.MILLI_OF_DAY;

public class StreamSqlConverter
        extends AbstractRowConverter<RowData, RowData, RowData, LogicalType> {

    private static final long serialVersionUID = 6652637680662065910L;

    private final Random random = new Random();

    public StreamSqlConverter(RowType rowType) {
        super(rowType);
        List<RowType.RowField> fields = rowType.getFields();
        for (RowType.RowField field : fields) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(createInternalConverter(field)));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(field), field.getType()));
        }
    }

    @Override
    protected ISerializationConverter<GenericRowData> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, rowData) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                rowData.setField(index, null);
            } else {
                serializationConverter.serialize(val, index, rowData);
            }
        };
    }

    @Override
    public RowData toInternal(RowData input) throws Exception {
        GenericRowData row = new GenericRowData(input.getArity());
        for (int i = 0; i < input.getArity(); i++) {
            row.setField(i, toInternalConverters.get(i).deserialize(input));
        }
        return row;
    }

    @Override
    public RowData toExternal(RowData rowData, RowData output) throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, output);
        }
        return output;
    }

    protected IDeserializationConverter createInternalConverter(RowType.RowField rowField) {
        LogicalType type = rowField.getType();
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return val -> JMockData.mock(boolean.class);
            case FLOAT:
                return val -> JMockData.mock(float.class);
            case DOUBLE:
                return val -> JMockData.mock(double.class);
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return val -> JMockData.mock(Time.class);
            case TINYINT:
                return val -> JMockData.mock(int.class).byteValue();
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return val -> JMockData.mock(int.class).shortValue();
            case INTEGER:
                return val -> JMockData.mock(int.class);
            case BIGINT:
                return val -> JMockData.mock(Long.class);
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal(JMockData.mock(BigInteger.class), 0),
                                        precision,
                                        scale)
                                : DecimalData.fromBigDecimal(
                                        JMockData.mock(BigDecimal.class), precision, scale);
            case DATE:
                return val -> (int) LocalDate.now().toEpochDay();
            case TIME_WITHOUT_TIME_ZONE:
                return val -> LocalTime.now().get(MILLI_OF_DAY);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromEpochMillis(System.currentTimeMillis());
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(JMockData.mock(String.class));
            case BINARY:
            case VARBINARY:
                return val -> JMockData.mock(byte[].class);
            case ARRAY:
                return val -> {
                    Object[] result = new Object[random.nextInt(10)];
                    LogicalType logicalType = type.getChildren().get(0);
                    RowType.RowField rowField1 = new RowType.RowField("", logicalType, "");
                    IDeserializationConverter internalConverter =
                            createInternalConverter(rowField1);
                    for (int i = 0; i < result.length; i++) {
                        Object value = internalConverter.deserialize(null);
                        result[i] = value;
                    }
                    return new GenericArrayData(result);
                };
            case ROW:
                return val -> {
                    List<RowType.RowField> childrenFields = ((RowType) type).getFields();
                    GenericRowData genericRowData = new GenericRowData(childrenFields.size());
                    for (int i = 0; i < childrenFields.size(); i++) {
                        Object value =
                                createInternalConverter(childrenFields.get(i)).deserialize(null);
                        genericRowData.setField(i, value);
                    }
                    return genericRowData;
                };
            case MAP:
                return val -> {
                    HashMap<Object, Object> resultMap = new HashMap<>();
                    LogicalType keyType = ((MapType) type).getKeyType();
                    LogicalType valueType = ((MapType) type).getValueType();
                    RowType.RowField keyRowField = new RowType.RowField("", keyType, "");
                    RowType.RowField valueRowField = new RowType.RowField("", valueType, "");
                    IDeserializationConverter keyInternalConverter =
                            createInternalConverter(keyRowField);
                    IDeserializationConverter valueInternalConverter =
                            createInternalConverter(valueRowField);
                    for (int i = 0; i < random.nextInt(5); i++) {
                        resultMap.put(
                                keyInternalConverter.deserialize(null),
                                valueInternalConverter.deserialize(null));
                    }

                    return new GenericMapData(resultMap);
                };
            case MULTISET:
                return val -> {
                    HashMap<Object, Object> resultMap = new HashMap<>();
                    LogicalType logicalType = type.getChildren().get(0);
                    RowType.RowField keyRowField = new RowType.RowField("", logicalType, "");
                    IDeserializationConverter keyInternalConverter =
                            createInternalConverter(keyRowField);
                    for (int i = 0; i < random.nextInt(5); i++) {
                        resultMap.put(keyInternalConverter.deserialize(null), 1);
                    }

                    return new GenericMapData(resultMap);
                };
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    protected ISerializationConverter<GenericRowData> createExternalConverter(
            RowType.RowField rowField) {
        LogicalType type = rowField.getType();
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
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int timestampPrecision;
                if (type instanceof LocalZonedTimestampType) {
                    timestampPrecision = ((LocalZonedTimestampType) type).getPrecision();
                } else {
                    timestampPrecision = ((TimestampType) type).getPrecision();
                }
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
                return (val, index, rowData) -> {
                    ArrayData array = val.getArray(index);
                    Object[] obj = new Object[array.size()];
                    ExternalDataUtil.arrayDataToExternal(type.getChildren().get(0), obj, array);
                    rowData.setField(index, GsonUtil.GSON.toJson(obj));
                };
            case MAP:
                return (val, index, rowData) -> {
                    MapData map = val.getMap(index);
                    Map<Object, Object> resultMap = new HashMap<>();
                    ExternalDataUtil.mapDataToExternal(
                            map,
                            ((MapType) type).getKeyType(),
                            ((MapType) type).getValueType(),
                            resultMap);
                    rowData.setField(index, GsonUtil.GSON.toJson(resultMap));
                };
            case MULTISET:
                return (val, index, rowData) -> {
                    MapData map = val.getMap(index);
                    ArrayData arrayData = map.keyArray();
                    Object[] obj = new Object[arrayData.size()];
                    ExternalDataUtil.arrayDataToExternal(type.getChildren().get(0), obj, arrayData);
                    rowData.setField(index, GsonUtil.GSON.toJson(obj));
                };
            case ROW:
                return (val, index, rowData) -> {
                    List<RowType.RowField> fields = ((RowType) type).getFields();
                    HashMap<String, Object> map = new HashMap<>();
                    for (int i = 0; i < fields.size(); i++) {
                        ExternalDataUtil.rowDataToExternal(
                                val.getRow(index, fields.size()),
                                i,
                                fields.get(i).getType(),
                                map,
                                fields.get(i).getName());
                    }
                    rowData.setField(index, GsonUtil.GSON.toJson(map));
                };
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
