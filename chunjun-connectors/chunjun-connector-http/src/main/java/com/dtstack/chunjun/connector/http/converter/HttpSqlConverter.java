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

package com.dtstack.chunjun.connector.http.converter;

import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.dtstack.chunjun.util.MapUtil;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.google.gson.internal.LinkedTreeMap;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalQueries;
import java.util.List;
import java.util.Map;

/** Base class for all converters that convert between restapi body and Flink internal object. */
public class HttpSqlConverter
        extends AbstractRowConverter<Map<String, Object>, RowData, RowData, LogicalType> {

    private static final long serialVersionUID = -9145005567073875082L;

    private HttpRestConfig httpRestConfig;

    public HttpSqlConverter(RowType rowType, HttpRestConfig httpRestConfig) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
        }
        this.httpRestConfig = httpRestConfig;
    }

    public HttpSqlConverter(RowType rowType) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(rowType.getTypeAt(i)), rowType.getTypeAt(i)));
        }
    }

    @Override
    public RowData toInternal(Map<String, Object> result) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        List<String> columns = rowType.getFieldNames();
        for (int pos = 0; pos < columns.size(); pos++) {
            Object value = MapUtil.getValueByKey(result, columns.get(pos), "");
            if (value instanceof LinkedTreeMap) {
                value = value.toString();
            }
            genericRowData.setField(pos, toInternalConverters.get(pos).deserialize(value));
        }
        return genericRowData;
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return (IDeserializationConverter<Boolean, Boolean>) Boolean::valueOf;
            case INTEGER:
                return (IDeserializationConverter<Integer, Integer>) Integer::valueOf;
            case BIGINT:
                return (IDeserializationConverter<String, Long>) Long::parseLong;
            case DATE:
                return (IDeserializationConverter<String, Integer>)
                        val -> {
                            LocalDate date =
                                    DateTimeFormatter.ISO_LOCAL_DATE
                                            .parse(val)
                                            .query(TemporalQueries.localDate());
                            return (int) date.toEpochDay();
                        };
            case FLOAT:
                return (IDeserializationConverter<Float, Float>) Float::valueOf;
            case DOUBLE:
                return (IDeserializationConverter<Double, Double>) Double::valueOf;
            case CHAR:
            case VARCHAR:
                return (IDeserializationConverter<String, StringData>) StringData::fromString;
            case DECIMAL:
                return (IDeserializationConverter<String, DecimalData>)
                        val -> {
                            final int precision = ((DecimalType) type).getPrecision();
                            final int scale = ((DecimalType) type).getScale();
                            return DecimalData.fromBigDecimal(
                                    new BigDecimal(val), precision, scale);
                        };
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
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
                return (val, index, rowData) -> rowData.setField(index, val.getInt(index));
            case BIGINT:
                return (val, index, rowData) -> rowData.setField(index, val.getLong(index));
            case FLOAT:
                return (val, index, rowData) -> rowData.setField(index, val.getFloat(index));
            case DOUBLE:
                return (val, index, rowData) -> rowData.setField(index, val.getDouble(index));
            case VARCHAR:
                // value is BinaryString
                return (val, index, rowData) ->
                        rowData.setField(index, val.getString(index).toString());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
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
    public RowData toExternal(RowData rowData, RowData output) throws Exception {
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, output);
        }
        return output;
    }
}
