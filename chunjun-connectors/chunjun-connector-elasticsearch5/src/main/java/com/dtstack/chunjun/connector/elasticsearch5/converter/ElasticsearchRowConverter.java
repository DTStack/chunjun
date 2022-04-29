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

package com.dtstack.chunjun.connector.elasticsearch5.converter;

import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.converter.ISerializationConverter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
 * @create: 2021/06/27 23:48
 */
public class ElasticsearchRowConverter
        extends AbstractRowConverter<
                Map<String, Object>, Map<String, Object>, Map<String, Object>, LogicalType> {

    private static final long serialVersionUID = 2L;

    private Logger LOG = LoggerFactory.getLogger(ElasticsearchRowConverter.class);
    private List<Tuple3<String, Integer, LogicalType>> typeIndexList = new ArrayList<>();

    public ElasticsearchRowConverter(RowType rowType) {
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
    public RowData toInternal(Map<String, Object> input) throws Exception {
        return genericRowData(input);
    }

    @Override
    public RowData toInternalLookup(Map<String, Object> input) throws Exception {
        return null;
    }

    private GenericRowData genericRowData(Map<String, Object> input) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (String key : input.keySet()) {
            List<Tuple3<String, Integer, LogicalType>> collect =
                    typeIndexList.stream()
                            .filter(x -> x._1().equals(key))
                            .collect(Collectors.toList());
            Tuple3<String, Integer, LogicalType> typeTuple = collect.get(0);
            genericRowData.setField(
                    typeTuple._2(),
                    toInternalConverters.get(typeTuple._2()).deserialize(input.get(key)));
        }
        return genericRowData;
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
    protected ISerializationConverter<Map<String, Object>> createExternalConverter(
            LogicalType type) {
        switch (type.getTypeRoot()) {
            case TINYINT:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index)._1(), val.getByte(index));
            case SMALLINT:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index)._1(), val.getShort(index));
            case INTEGER:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index)._1(), val.getInt(index));
            case BIGINT:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index)._1(), val.getLong(index));
            case FLOAT:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index)._1(), val.getFloat(index));
            case DOUBLE:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index)._1(), val.getDouble(index));
            case DECIMAL:
                return (val, index, output) ->
                        output.put(
                                typeIndexList.get(index)._1(),
                                val.getDecimal(index, 10, 8).toBigDecimal());
            case VARCHAR:
            case CHAR:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index)._1(), val.getString(index).toString());
            case BOOLEAN:
                return (val, index, output) ->
                        output.put(typeIndexList.get(index)._1(), val.getBoolean(index));
            case DATE:
                return (val, index, output) -> {
                    output.put(
                            typeIndexList.get(index)._1(),
                            Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))).toString());
                };
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, output) -> {
                    try {
                        String result =
                                Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L))
                                        .toString();
                        output.put(typeIndexList.get(index)._1(), result);
                    } catch (Exception e) {
                        LOG.error("converter error. Value: {}, Type: {}", val, type.getTypeRoot());
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
                        output.put(typeIndexList.get(index)._1(), result);
                    } catch (Exception e) {
                        LOG.error("converter error. Value: {}, Type: {}", val, type.getTypeRoot());
                        throw new RuntimeException("Converter error.", e);
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {

        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return val -> new Boolean(String.valueOf(val));
            case FLOAT:
                return val -> new Float(String.valueOf(val));
            case DOUBLE:
                return val -> new Double(String.valueOf(val));
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return val -> Time.valueOf(String.valueOf(val));
            case INTEGER:
                return val -> new Integer(String.valueOf(val));
            case BIGINT:
                return val -> new Long(String.valueOf(val));
            case TINYINT:
                return val -> new Integer(String.valueOf(val)).byteValue();
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return val -> new Integer(String.valueOf(val)).shortValue();
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                        val instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                        new BigDecimal((BigInteger) val, 0), precision, scale)
                                : DecimalData.fromBigDecimal(
                                        new BigDecimal(String.valueOf(val)), precision, scale);
            case DATE:
                return val ->
                        (int) ((Date.valueOf(String.valueOf(val))).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val ->
                        (int)
                                ((Time.valueOf(String.valueOf(val))).toLocalTime().toNanoOfDay()
                                        / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val -> TimestampData.fromTimestamp(Timestamp.valueOf(String.valueOf(val)));
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString(val.toString());
            case BINARY:
            case VARBINARY:
                return val -> (byte[]) val;
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
