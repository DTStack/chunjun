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

package com.dtstack.flinkx.connector.restapi.convert;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.restapi.client.DefaultRestHandler;
import com.dtstack.flinkx.connector.restapi.common.HttpRestConfig;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.MapUtil;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.google.gson.internal.LinkedTreeMap;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalQueries;
import java.util.List;
import java.util.Map;

/** Base class for all converters that convert between restapi body and Flink internal object. */
public class RestapiRowConverter extends AbstractRowConverter<String, Object, Object, LogicalType> {

    private HttpRestConfig httpRestConfig;

    public RestapiRowConverter(RowType rowType, HttpRestConfig httpRestConfig) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters[i] =
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i)));
        }
        this.httpRestConfig = httpRestConfig;
    }

    @Override
    public RowData toInternal(String input) throws Exception {
        Map<String, Object> result =
                DefaultRestHandler.gson.fromJson(input, GsonUtil.gsonMapTypeToken);
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        List<FieldConf> column = httpRestConfig.getColumn();
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            Object value =
                    MapUtil.getValueByKey(
                            result, column.get(pos).getName(), httpRestConfig.getFieldDelimiter());
            if (value instanceof LinkedTreeMap) {
                value = value.toString();
            }
            genericRowData.setField(pos, toInternalConverters[pos].deserialize(value));
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
    protected ISerializationConverter createExternalConverter(LogicalType type) {
        return null;
    }

    @Override
    protected ISerializationConverter wrapIntoNullableExternalConverter(
            ISerializationConverter ISerializationConverter, LogicalType type) {
        return null;
    }

    @Override
    public RowData toInternalLookup(Object input) {
        return null;
    }

    @Override
    public Object toExternal(RowData rowData, Object output) {
        return null;
    }
}
