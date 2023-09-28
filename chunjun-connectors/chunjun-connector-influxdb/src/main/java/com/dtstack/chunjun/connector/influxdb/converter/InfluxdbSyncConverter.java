/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.connector.influxdb.converter;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.element.AbstractBaseColumn;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.NullColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.dto.Point;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxdbSyncConverter
        extends AbstractRowConverter<Map<String, Object>, RowData, Point.Builder, LogicalType> {
    private static final long serialVersionUID = 7359032828293825689L;

    private static final String TIME_KEY = "time";

    private final List<String> fieldNameList;
    private final List<FieldConfig> fieldConfList;
    private final TimeUnit precision;
    private String format = "MSGPACK";
    private List<String> tags;
    private String timestamp;

    public InfluxdbSyncConverter(
            RowType rowType,
            CommonConfig commonConfig,
            List<String> fieldNameList,
            String format,
            TimeUnit precision) {
        super(rowType, commonConfig);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
        this.format = format;
        this.fieldConfList = commonConfig.getColumn();
        this.fieldNameList = fieldNameList;
        this.precision = precision;
    }

    public InfluxdbSyncConverter(
            RowType rowType,
            CommonConfig commonConfig,
            List<String> fieldNameList,
            List<String> tags,
            String timestamp,
            TimeUnit precision) {
        super(rowType, commonConfig);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
        this.fieldConfList = commonConfig.getColumn();
        this.fieldNameList = fieldNameList;
        this.tags = tags;
        this.timestamp = timestamp;
        this.precision = precision;
    }

    @Override
    protected ISerializationConverter<Point.Builder> wrapIntoNullableExternalConverter(
            ISerializationConverter<Point.Builder> converter, LogicalType type) {
        return (val, index, builder) -> {
            if (val == null || val.isNullAt(index)) {
                return;
            } else {
                converter.serialize(val, index, builder);
            }
        };
    }

    @Override
    public RowData toInternal(Map<String, Object> input) throws Exception {
        int converterIndex = 0;
        if (fieldConfList.size() == 1
                && StringUtils.equals(ConstantValue.STAR_SYMBOL, fieldConfList.get(0).getName())) {
            ColumnRowData result = new ColumnRowData(fieldNameList.size());
            for (String fieldName : fieldNameList) {
                AbstractBaseColumn baseColumn = setValue(input, fieldName, converterIndex);
                result.addField(baseColumn);
                converterIndex++;
            }
            return result;
        } else {
            ColumnRowData result = new ColumnRowData(fieldConfList.size());
            for (FieldConfig fieldConfig : fieldConfList) {
                String fieldName = fieldConfig.getName();
                AbstractBaseColumn baseColumn = setValue(input, fieldName, converterIndex);
                result.addField(assembleFieldProps(fieldConfig, baseColumn));
                converterIndex++;
            }
            return result;
        }
    }

    /**
     * Set the value of input into column.
     *
     * @param input input value.
     * @param fieldName field name of input.
     * @param index index of converter.
     * @return column
     * @throws Exception the exception from converter.
     */
    private AbstractBaseColumn setValue(Map<String, Object> input, String fieldName, int index)
            throws Exception {
        AbstractBaseColumn baseColumn;
        if (TIME_KEY.equalsIgnoreCase(fieldName)) {
            Long timeLong = (Long) input.get(fieldName);
            long timeMs = TimeUnit.MILLISECONDS.convert(timeLong, precision);
            baseColumn = new TimestampColumn(timeMs);
        } else {
            Object field = input.get(fieldName);
            baseColumn = (AbstractBaseColumn) toInternalConverters.get(index).deserialize(field);
        }
        return baseColumn;
    }

    @Override
    public Point.Builder toExternal(RowData rowData, Point.Builder output) throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            if (!specicalField(fieldNameList.get(index), rowData, index, output))
                toExternalConverters.get(index).serialize(rowData, index, output);
        }
        return output;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return val -> new BooleanColumn(Boolean.parseBoolean(val.toString()));
            case INTEGER:
                return val -> {
                    if ("JSON".equals(format)) {
                        return new IntColumn(((Double) val).intValue());
                    }
                    return new IntColumn((Integer) val);
                };
            case FLOAT:
                return val -> new FloatColumn(((Double) val).floatValue());
            case DOUBLE:
                return val -> new DoubleColumn((Double) val);
            case VARBINARY:
                return val -> new BytesColumn((byte[]) val);
            case BIGINT:
                return val -> {
                    if ("JSON".equals(format)) {
                        return new LongColumn(((Double) val).longValue());
                    }
                    if (val instanceof Long) {
                        return new LongColumn((Long) val);
                    } else {
                        return new LongColumn((Integer) val);
                    }
                };

            case VARCHAR:
                return val -> new StringColumn((String) val);
            case NULL:
                return val -> new NullColumn();
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<Point.Builder> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case VARCHAR:
                return (val, index, builder) -> {
                    builder.addField(fieldNameList.get(index), val.getString(index).toString());
                };
            case FLOAT:
                return (val, index, builder) -> {
                    builder.addField(fieldNameList.get(index), val.getDouble(index));
                };
            case INTEGER:
                return (val, index, builder) -> {
                    builder.addField(fieldNameList.get(index), val.getLong(index));
                };
            case BOOLEAN:
                return (val, index, builder) -> {
                    builder.addField(fieldNameList.get(index), val.getBoolean(index));
                };
            case BIGINT:
                return (val, index, builder) -> {
                    // Only timstamp support be bigint,however it is processed in specicalField
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private boolean specicalField(
            String fieldName, RowData value, int index, Point.Builder builder) {
        if (StringUtils.isNotBlank(fieldName)) {
            if (fieldName.equals(timestamp)) {
                builder.time(value.getLong(index), precision);
                return true;
            } else if (CollectionUtils.isNotEmpty(tags) && tags.contains(fieldName)) {
                if (!value.isNullAt(index))
                    builder.tag(fieldName, ((ColumnRowData) value).getField(index).asString());
                return true;
            }
        }
        return false;
    }
}
