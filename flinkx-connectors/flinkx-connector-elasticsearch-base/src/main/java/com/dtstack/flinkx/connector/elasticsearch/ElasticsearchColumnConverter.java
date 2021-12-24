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

package com.dtstack.flinkx.connector.elasticsearch;

import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.util.DateUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.collections.CollectionUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple3;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 13:23
 */
public class ElasticsearchColumnConverter
        extends AbstractRowConverter<
                Map<String, Object>, Object, Map<String, Object>, LogicalType> {

    private static final long serialVersionUID = 2L;

    private final List<Tuple3<String, Integer, LogicalType>> typeIndexList = new ArrayList<>();

    public ElasticsearchColumnConverter(RowType rowType) {
        super(rowType);
        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
            typeIndexList.add(new Tuple3<>(fieldNames.get(i), i, rowType.getTypeAt(i)));
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
                result.put(typeIndexList.get(index)._1(), null);
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
            List<Tuple3<String, Integer, LogicalType>> collect =
                    typeIndexList.stream()
                            .filter(x -> x._2() == index)
                            .collect(Collectors.toList());

            if (CollectionUtils.isEmpty(collect)) {
                LOG.warn("Result Map : key [{}] not in columns", typeIndexList.get(index)._2());
                continue;
            }

            Tuple3<String, Integer, LogicalType> typeTuple = collect.get(0);
            Object field = input.get(typeTuple._1());
            columnRowData.addField(
                    (AbstractBaseColumn) toInternalConverters.get(i).deserialize(field));
        }
        return columnRowData;
    }

    @Override
    public Map<String, Object> toExternal(RowData rowData, Map<String, Object> output)
            throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
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
                return val -> new BigDecimalColumn(((Integer) val).byteValue());
            case SMALLINT:
            case INTEGER:
                return val -> new BigDecimalColumn((Integer) val);
            case FLOAT:
                return val -> new BigDecimalColumn((Float) val);
            case DOUBLE:
                return val -> new BigDecimalColumn((Double) val);
            case BIGINT:
                return val -> new BigDecimalColumn((Long) val);
            case DECIMAL:
                return val -> new BigDecimalColumn(BigDecimal.valueOf((Double) val));
            case CHAR:
            case VARCHAR:
                return val -> new StringColumn((String) val);
            case DATE:
                return val ->
                        new BigDecimalColumn(
                                Date.valueOf(String.valueOf(val)).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        new BigDecimalColumn(
                                Time.valueOf(String.valueOf(val)).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> new TimestampColumn(DateUtil.getTimestampFromStr(val.toString()));
            case BINARY:
            case VARBINARY:
                return val -> new BytesColumn((byte[]) val);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<Map<String, Object>> createExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, output) -> {
                    output.put(
                            typeIndexList.get(index)._1(),
                            ((ColumnRowData) val).getField(index).asBoolean());
                };
            case TINYINT:
                return (val, index, output) -> {
                    output.put(typeIndexList.get(index)._1(), val.getByte(index));
                };
            case SMALLINT:
            case INTEGER:
                return (val, index, output) -> {
                    output.put(
                            typeIndexList.get(index)._1(),
                            ((ColumnRowData) val).getField(index).asInt());
                };
            case FLOAT:
                return (val, index, output) -> {
                    output.put(
                            typeIndexList.get(index)._1(),
                            ((ColumnRowData) val).getField(index).asFloat());
                };
            case DOUBLE:
                return (val, index, output) -> {
                    output.put(
                            typeIndexList.get(index)._1(),
                            ((ColumnRowData) val).getField(index).asDouble());
                };

            case BIGINT:
                return (val, index, output) -> {
                    output.put(
                            typeIndexList.get(index)._1(),
                            ((ColumnRowData) val).getField(index).asLong());
                };
            case DECIMAL:
                return (val, index, output) -> {
                    output.put(
                            typeIndexList.get(index)._1(),
                            ((ColumnRowData) val).getField(index).asBigDecimal());
                };
            case CHAR:
            case VARCHAR:
                return (val, index, output) -> {
                    output.put(
                            typeIndexList.get(index)._1(),
                            ((ColumnRowData) val).getField(index).asString());
                };
            case DATE:
                return (val, index, output) -> {
                    output.put(
                            typeIndexList.get(index)._1(),
                            Date.valueOf(
                                            LocalDate.ofEpochDay(
                                                    ((ColumnRowData) val).getField(index).asInt()))
                                    .toString());
                };
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, output) -> {
                    output.put(
                            typeIndexList.get(index)._1(),
                            Time.valueOf(
                                            LocalTime.ofNanoOfDay(
                                                    ((ColumnRowData) val).getField(index).asInt()
                                                            * 1_000_000L))
                                    .toString());
                };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, output) -> {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    output.put(
                            typeIndexList.get(index)._1(),
                            sdf.format(((ColumnRowData) val).getField(index).asDate()));
                };
            case BINARY:
            case VARBINARY:
                return (val, index, output) -> {
                    output.put(
                            typeIndexList.get(index)._1(),
                            ((ColumnRowData) val).getField(index).asBytes());
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
