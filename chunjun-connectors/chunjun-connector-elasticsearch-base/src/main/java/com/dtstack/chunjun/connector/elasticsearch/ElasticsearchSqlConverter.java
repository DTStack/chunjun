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

package com.dtstack.chunjun.connector.elasticsearch;

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.util.ExternalDataUtil;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class ElasticsearchSqlConverter
        extends AbstractRowConverter<
                Map<String, Object>, Map<String, Object>, Map<String, Object>, LogicalType> {

    private static final long serialVersionUID = -1093338072559909026L;
    private final List<Tuple2<String, Integer>> typeIndexList = new ArrayList<>();

    public ElasticsearchSqlConverter(RowType rowType) {
        super(rowType);
        List<RowType.RowField> fields = rowType.getFields();

        for (int i = 0; i < fields.size(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(createInternalConverter(fields.get(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fields.get(i)), fields.get(i).getType()));
            typeIndexList.add(new Tuple2<>(fields.get(i).getName(), i));
        }
    }

    @Override
    protected ISerializationConverter wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, rowData) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                Map<String, Object> result = (Map<String, Object>) rowData;
                result.put(typeIndexList.get(index).f0, null);
            } else {
                serializationConverter.serialize(val, index, rowData);
            }
        };
    }

    @Override
    public RowData toInternal(Map<String, Object> input) throws Exception {
        return genericRowData(input);
    }

    @Override
    public RowData toInternalLookup(Map<String, Object> input) throws Exception {
        return genericRowData(input);
    }

    private GenericRowData genericRowData(Map<String, Object> input) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (String key : input.keySet()) {
            List<Tuple2<String, Integer>> collect =
                    typeIndexList.stream()
                            .filter(x -> x.f0.equals(key))
                            .collect(Collectors.toList());

            if (CollectionUtils.isEmpty(collect)) {
                log.warn("Result Map : key [{}] not in columns", key);
                continue;
            }

            Tuple2<String, Integer> typeTuple = collect.get(0);
            genericRowData.setField(
                    typeTuple.f1,
                    toInternalConverters.get(typeTuple.f1).deserialize(input.get(key)));
        }
        return genericRowData;
    }

    @Override
    public Map<String, Object> toExternal(RowData rowData, Map<String, Object> output)
            throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, output);
        }
        return output;
    }

    protected ISerializationConverter<Map<String, Object>> createExternalConverter(
            RowType.RowField rowField) {
        LogicalType type = rowField.getType();
        switch (type.getTypeRoot()) {
            case TINYINT:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index).f0, val.getByte(index));
            case SMALLINT:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index).f0, val.getShort(index));
            case INTEGER:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index).f0, val.getInt(index));
            case BIGINT:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index).f0, val.getLong(index));
            case FLOAT:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index).f0, val.getFloat(index));
            case DOUBLE:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index).f0, val.getDouble(index));
            case DECIMAL:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                val.getDecimal(index, 10, 8).toBigDecimal());
            case VARCHAR:
            case CHAR:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index).f0, val.getString(index).toString());
            case BOOLEAN:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index).f0, val.getBoolean(index));
            case DATE:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))).toString());
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, output) -> {
                    try {
                        String result =
                                Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L))
                                        .toString();
                        output.put(typeIndexList.get(index).f0, result);
                    } catch (Exception e) {
                        log.error("converter error. Value: {}, Type: {}", val, type.getTypeRoot());
                        throw new RuntimeException("Converter error.", e);
                    }
                };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, output) -> {
                    String result;
                    try {
                        final int timestampPrecision = ((TimestampType) type).getPrecision();
                        result =
                                val.getTimestamp(index, timestampPrecision)
                                        .toTimestamp()
                                        .toString();
                        output.put(typeIndexList.get(index).f0, result);
                    } catch (Exception e) {
                        log.error("converter error. Value: {}, Type: {}", val, type.getTypeRoot());
                        throw new RuntimeException("Converter error.", e);
                    }
                };
            case ROW:
                return (val, index, output) -> {
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
                    output.put(typeIndexList.get(index).f0, map);
                };

            case ARRAY:
                return (val, index, output) -> {
                    ArrayData array = val.getArray(index);
                    Object[] obj = new Object[array.size()];
                    ExternalDataUtil.arrayDataToExternal(type.getChildren().get(0), obj, array);
                    output.put(typeIndexList.get(index).f0, obj);
                };
            case MAP:
                return (val, index, output) -> {
                    MapData map = val.getMap(index);
                    Map<Object, Object> resultMap = new HashMap<>();
                    ExternalDataUtil.mapDataToExternal(
                            map,
                            ((MapType) type).getKeyType(),
                            ((MapType) type).getValueType(),
                            resultMap);
                    output.put(typeIndexList.get(index).f0, resultMap);
                };
            case MULTISET:
                return (val, index, output) -> {
                    MapData map = val.getMap(index);
                    ArrayData arrayData = map.keyArray();
                    Object[] obj = new Object[arrayData.size()];
                    ExternalDataUtil.arrayDataToExternal(type.getChildren().get(0), obj, arrayData);
                    output.put(typeIndexList.get(index).f0, obj);
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    protected IDeserializationConverter createInternalConverter(RowType.RowField rowField) {
        LogicalType type = rowField.getType();
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return val -> val == null ? null : Boolean.valueOf(String.valueOf(val));
            case FLOAT:
                return val -> val == null ? null : new Float(String.valueOf(val));
            case DOUBLE:
                return val -> val == null ? null : new Double(String.valueOf(val));
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return val -> val == null ? null : Time.valueOf(String.valueOf(val));
            case INTEGER:
                return val -> val == null ? null : new Integer(String.valueOf(val));
            case BIGINT:
                return val -> val == null ? null : new Long(String.valueOf(val));
            case TINYINT:
                return val -> val == null ? null : new Integer(String.valueOf(val)).byteValue();
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return val -> val == null ? null : new Integer(String.valueOf(val)).shortValue();
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val == null
                                ? null
                                : val instanceof BigInteger
                                        ? DecimalData.fromBigDecimal(
                                                new BigDecimal((BigInteger) val, 0),
                                                precision,
                                                scale)
                                        : DecimalData.fromBigDecimal(
                                                new BigDecimal(String.valueOf(val)),
                                                precision,
                                                scale);
            case DATE:
                return val ->
                        val == null
                                ? null
                                : (int)
                                        ((Date.valueOf(String.valueOf(val)))
                                                .toLocalDate()
                                                .toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        val == null
                                ? null
                                : (int)
                                        ((Time.valueOf(String.valueOf(val)))
                                                        .toLocalTime()
                                                        .toNanoOfDay()
                                                / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val ->
                        val == null
                                ? null
                                : TimestampData.fromTimestamp(
                                        Timestamp.valueOf(String.valueOf(val)));
            case CHAR:
            case VARCHAR:
                return val -> val == null ? null : StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> val == null ? null : (byte[]) val;
            case ROW:
                return val -> {
                    List<RowType.RowField> childrenFields = ((RowType) type).getFields();
                    Map<String, Object> val1 = (Map<String, Object>) val;
                    GenericRowData genericRowData = new GenericRowData(childrenFields.size());
                    for (int i = 0; i < childrenFields.size(); i++) {
                        if (val1.get(childrenFields.get(i).getName()) == null) {
                            genericRowData.setField(i, null);
                        } else {
                            Object value =
                                    createInternalConverter(childrenFields.get(i))
                                            .deserialize(val1.get(childrenFields.get(i).getName()));
                            genericRowData.setField(i, value);
                        }
                    }
                    return genericRowData;
                };
            case ARRAY:
                return (val) -> {
                    ArrayList<Object> list = (ArrayList<Object>) val;
                    Object[] result = new Object[list.size()];
                    LogicalType logicalType = type.getChildren().get(0);
                    RowType.RowField rowField1 = new RowType.RowField("", logicalType, "");
                    IDeserializationConverter internalConverter =
                            createInternalConverter(rowField1);
                    for (int i = 0; i < list.size(); i++) {
                        if (list.get(i) == null) {
                            result[i] = null;
                        } else {
                            Object value = internalConverter.deserialize(list.get(i));
                            result[i] = value;
                        }
                    }
                    return new GenericArrayData(result);
                };
            case MAP:
                return val -> {
                    if (val == null) {
                        return null;
                    }
                    HashMap<Object, Object> resultMap = new HashMap<>();
                    Map map = GsonUtil.GSON.fromJson(val.toString(), Map.class);
                    LogicalType keyType = ((MapType) type).getKeyType();
                    LogicalType valueType = ((MapType) type).getValueType();
                    RowType.RowField keyRowField = new RowType.RowField("", keyType, "");
                    RowType.RowField valueRowField = new RowType.RowField("", valueType, "");
                    IDeserializationConverter keyInternalConverter =
                            createInternalConverter(keyRowField);
                    IDeserializationConverter valueInternalConverter =
                            createInternalConverter(valueRowField);
                    for (Object key : map.keySet()) {
                        resultMap.put(
                                keyInternalConverter.deserialize(key),
                                valueInternalConverter.deserialize(map.get(key)));
                    }

                    return new GenericMapData(resultMap);
                };
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
