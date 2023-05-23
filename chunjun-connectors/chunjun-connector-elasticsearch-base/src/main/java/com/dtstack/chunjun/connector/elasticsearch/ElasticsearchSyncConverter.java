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
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.throwable.CastException;
import com.dtstack.chunjun.util.DateUtil;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class ElasticsearchSyncConverter
        extends AbstractRowConverter<
                Map<String, Object>, Object, Map<String, Object>, LogicalType> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long serialVersionUID = -4080093751566715519L;

    private final List<Tuple2<String, Integer>> typeIndexList = new ArrayList<>();
    private final Map<Integer, SimpleDateFormat> dateFormatMap = new HashMap<>();

    public ElasticsearchSyncConverter(RowType rowType) {
        super(rowType);
        List<RowType.RowField> fieldList = rowType.getFields();
        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            // 获取description中的format信息
            RowType.RowField rowField = fieldList.get(i);
            Optional<String> description = rowField.getDescription();
            if (fieldTypes[i].getTypeRoot().equals(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
                if (description.isPresent()) {
                    dateFormatMap.put(i, new SimpleDateFormat(description.get()));
                }
            }
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
            typeIndexList.add(new Tuple2<>(fieldNames.get(i), i));
        }
    }

    @Override
    protected ISerializationConverter wrapIntoNullableExternalConverter(
            ISerializationConverter ISerializationConverter, LogicalType type) {
        return (val, index, rowData) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                Map<String, Object> result = (Map<String, Object>) rowData;
                result.put(typeIndexList.get(index).f0, null);
            } else {
                ISerializationConverter.serialize(val, index, rowData);
            }
        };
    }

    @Override
    public RowData toInternal(Map<String, Object> input) throws Exception {
        ColumnRowData columnRowData = new ColumnRowData(rowType.getFieldCount());
        for (int i = 0; i < toInternalConverters.size(); i++) {
            final int index = i;
            List<Tuple2<String, Integer>> collect =
                    typeIndexList.stream().filter(x -> x.f1 == index).collect(Collectors.toList());

            if (CollectionUtils.isEmpty(collect)) {
                log.warn("Result Map : key [{}] not in columns", typeIndexList.get(index).f1);
                continue;
            }

            Tuple2<String, Integer> typeTuple = collect.get(0);
            Object field = input.get(typeTuple.f0);
            columnRowData.addField(
                    (AbstractBaseColumn) toInternalConverters.get(i).deserialize(field));
        }
        return columnRowData;
    }

    @Override
    public Map<String, Object> toExternal(RowData rowData, Map<String, Object> output)
            throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, output);
        }
        return output;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {

        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case TINYINT:
                return val -> new ByteColumn(Byte.parseByte(val.toString()));
            case SMALLINT:
                return val -> new ShortColumn(Integer.valueOf(val.toString()).shortValue());
            case INTEGER:
                return val -> new IntColumn(Integer.parseInt(val.toString()));
            case FLOAT:
                return val -> new FloatColumn(Float.parseFloat(val.toString()));
            case DOUBLE:
                return val -> new DoubleColumn(Double.parseDouble(val.toString()));
            case BIGINT:
                return val -> new LongColumn(Long.parseLong(val.toString()));
            case DECIMAL:
                return val -> new BigDecimalColumn(new BigDecimal(val.toString()));
            case CHAR:
            case VARCHAR:
                return val -> new StringColumn(val.toString());
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> {
                    String valStr = val.toString();
                    try {
                        return new TimestampColumn(
                                Timestamp.valueOf(valStr),
                                DateUtil.getPrecisionFromTimestampStr(valStr));
                    } catch (Exception e) {
                        return new TimestampColumn(DateUtil.getTimestampFromStr(valStr), 0);
                    }
                };
            case TIME_WITHOUT_TIME_ZONE:
                return val -> new TimeColumn(Time.valueOf(String.valueOf(val)));
            case BINARY:
            case VARBINARY:
                return val -> new BytesColumn((byte[]) val);
            case STRUCTURED_TYPE:
                return val -> new StringColumn(objectMapper.writeValueAsString(val));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<Map<String, Object>> createExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                ((ColumnRowData) val).getField(index).asBoolean());
            case TINYINT:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index).f0, val.getByte(index));
            case SMALLINT:
            case INTEGER:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                ((ColumnRowData) val).getField(index).asInt());
            case FLOAT:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                ((ColumnRowData) val).getField(index).asFloat());
            case DOUBLE:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                ((ColumnRowData) val).getField(index).asDouble());

            case BIGINT:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                ((ColumnRowData) val).getField(index).asLong());
            case DECIMAL:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                ((ColumnRowData) val).getField(index).asBigDecimal());
            case CHAR:
            case VARCHAR:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                ((ColumnRowData) val).getField(index).asString());
            case DATE:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                ((ColumnRowData) val).getField(index).asSqlDate().toString());
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                ((ColumnRowData) val).getField(index).asTime().toString());
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, output) -> {
                    AbstractBaseColumn field = ((ColumnRowData) val).getField(index);
                    if (field instanceof StringColumn && ((StringColumn) field).isCustomFormat()
                            || null == dateFormatMap.get(index)) {
                        output.put(typeIndexList.get(index).f0, field.asTimestampStr());
                    } else {
                        output.put(
                                typeIndexList.get(index).f0,
                                dateFormatMap.get(index).format(field.asTimestamp().getTime()));
                    }
                };
            case BINARY:
            case VARBINARY:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index).f0,
                                ((ColumnRowData) val).getField(index).asBytes());
            case STRUCTURED_TYPE:
                return (val, index, output) -> {
                    String field = ((ColumnRowData) val).getField(index).asString();
                    try {
                        output.put(
                                typeIndexList.get(index).f0,
                                GsonUtil.GSON.fromJson(field, Map.class));
                    } catch (Exception e) {
                        try {
                            output.put(
                                    typeIndexList.get(index).f0,
                                    GsonUtil.GSON.fromJson(field, List.class));
                        } catch (Exception e1) {
                            throw new CastException("String", "Es Complex Type", field);
                        }
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
