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

package com.dtstack.chunjun.connector.redis.converter;

import com.dtstack.chunjun.connector.redis.config.RedisConfig;
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

import redis.clients.jedis.commands.JedisCommands;

import java.io.Serializable;
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

public class RedisSqlConverter
        extends AbstractRowConverter<
                Map<String, String>, Map<String, String>, JedisCommands, LogicalType> {

    private static final long serialVersionUID = -276443152802654355L;

    private RedisConfig redisConfig;

    private final List<Triplet<String, Integer, LogicalType>> typeIndexList = new ArrayList<>();

    public RedisSqlConverter(RowType rowType) {
        super(rowType);
        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toInternalConverters.add(
                    wrapIntoNullableInternalConverter(
                            createInternalConverter(rowType.getTypeAt(i))));
            typeIndexList.add(new Triplet<>(fieldNames.get(i), i));
        }
    }

    public RedisSqlConverter(RowType rowType, RedisConfig redisConfig) {
        super(rowType);
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            toExternalConverters.add(
                    wrapIntoNullableExternalConverter(
                            createExternalConverter(fieldTypes[i]), fieldTypes[i]));
        }
        this.redisConfig = redisConfig;
    }

    @Override
    protected ISerializationConverter<List<Object>> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (val, index, result) -> {
            if (val == null
                    || val.isNullAt(index)
                    || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                result.add(null);
            } else {
                serializationConverter.serialize(val, index, result);
            }
        };
    }

    @Override
    public RowData toInternal(Map<String, String> input) throws Exception {
        return getGenericRowData(input);
    }

    @Override
    public RowData toInternalLookup(Map<String, String> input) throws Exception {
        return getGenericRowData(input);
    }

    private GenericRowData getGenericRowData(Map<String, String> input) throws Exception {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());

        for (String key : input.keySet()) {
            List<Triplet<String, Integer, LogicalType>> collect =
                    typeIndexList.stream()
                            .filter(x -> x.first.equals(key))
                            .collect(Collectors.toList());
            if (!collect.isEmpty()) {
                Triplet<String, Integer, LogicalType> typeTriplet = collect.get(0);
                genericRowData.setField(
                        typeTriplet.second,
                        toInternalConverters.get(typeTriplet.second).deserialize(input.get(key)));
            }
        }

        return genericRowData;
    }

    @Override
    public JedisCommands toExternal(RowData rowData, JedisCommands jedis) throws Exception {
        List<String> fieldNames = rowType.getFieldNames();
        List<Object> fieldValue = new ArrayList<>();
        for (int index = 0; index < fieldTypes.length; index++) {
            toExternalConverters.get(index).serialize(rowData, index, fieldValue);
        }

        Map<String, Object> collect = new HashMap<>();
        fieldNames.forEach(
                key -> {
                    collect.put(key, fieldValue.get(fieldNames.indexOf(key)));
                });
        String key = buildCacheKey(collect);
        collect.forEach((field, value) -> jedis.hset(key, field, String.valueOf(value)));

        if (redisConfig.getExpireTime() != 0) {
            jedis.expire(key, (int) redisConfig.getExpireTime());
        }
        return jedis;
    }

    private String buildCacheKey(Map<String, Object> refData) {
        StringBuilder keyBuilder = new StringBuilder(redisConfig.getTableName());
        for (String primaryKey : redisConfig.getUpdateKey()) {
            if (!refData.containsKey(primaryKey)) {
                return null;
            }
            keyBuilder.append("_").append(refData.get(primaryKey));
        }
        return keyBuilder.toString();
    }

    @Override
    protected IDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return val -> Boolean.valueOf(String.valueOf(val));
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

    @Override
    protected ISerializationConverter<List<Object>> createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, result) -> result.add(val.getBoolean(index));
            case TINYINT:
                return (val, index, result) -> result.add(val.getByte(index));
            case SMALLINT:
                return (val, index, result) -> result.add(val.getShort(index));
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, index, result) -> result.add(val.getInt(index));
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, index, result) -> result.add(val.getLong(index));
            case FLOAT:
                return (val, index, result) -> result.add(val.getFloat(index));
            case DOUBLE:
                return (val, index, result) -> result.add(val.getDouble(index));
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (val, index, result) -> result.add(val.getString(index).toString());
            case BINARY:
            case VARBINARY:
                return (val, index, result) -> result.add(val.getBinary(index));
            case DATE:
                return (val, index, result) ->
                        result.add(Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, result) ->
                        result.add(
                                Time.valueOf(
                                        LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index, result) ->
                        result.add(val.getTimestamp(index, timestampPrecision).toTimestamp());
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index, result) ->
                        result.add(
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

    static class Triplet<T, U, V> implements Serializable {

        private static final long serialVersionUID = 1L;

        private final T first;
        private final U second;

        public Triplet(T first, U second) {
            this.first = first;
            this.second = second;
        }
    }
}
