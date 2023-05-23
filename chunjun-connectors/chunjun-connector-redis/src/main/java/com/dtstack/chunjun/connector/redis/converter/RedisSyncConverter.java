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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.redis.config.RedisConfig;
import com.dtstack.chunjun.connector.redis.enums.RedisDataMode;
import com.dtstack.chunjun.connector.redis.enums.RedisDataType;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.dtstack.chunjun.connector.redis.options.RedisOptions.REDIS_CRITICAL_TIME;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.REDIS_KEY_VALUE_SIZE;

public class RedisSyncConverter extends AbstractRowConverter<Object, Object, Jedis, LogicalType> {

    private static final long serialVersionUID = 3573774552923872927L;

    /** redis Conf */
    private final RedisConfig redisConfig;
    /** SimpleDateFormat */
    private SimpleDateFormat sdf;

    /** The index of column can be used as a field when mode is hash and Column is not empty */
    private List<Integer> fieldIndex = new ArrayList<>(2);

    public RedisSyncConverter(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
        if (StringUtils.isNotBlank(redisConfig.getDateFormat())) {
            sdf = new SimpleDateFormat(redisConfig.getDateFormat());
        }
        if (redisConfig.getType() == RedisDataType.HASH
                && CollectionUtils.isNotEmpty(redisConfig.getColumn())) {
            fieldIndex = new ArrayList<>(redisConfig.getColumn().size());
            for (int i = 0; i < redisConfig.getColumn().size(); i++) {
                fieldIndex.add(i);
            }
            if (!redisConfig.isIndexFillHash()
                    && CollectionUtils.isNotEmpty(redisConfig.getKeyIndexes())) {
                for (int index : redisConfig.getKeyIndexes()) {
                    fieldIndex.remove(index);
                }
            }
        }
    }

    @Override
    public RowData toInternal(Object input) {
        Map<String, String> map = (Map<String, String>) input;
        List<FieldConfig> column = redisConfig.getColumn();
        ColumnRowData rowData = new ColumnRowData(column.size());
        for (FieldConfig fieldConfig : column) {
            StringColumn value = new StringColumn(map.get(fieldConfig.getName()));
            rowData.addField(value);
        }
        return rowData;
    }

    @Override
    public Jedis toExternal(RowData rowData, Jedis jedis) {
        ColumnRowData row = (ColumnRowData) rowData;
        processTimeFormat(row);
        String key = concatKey(row);
        String[] values = getValues(row);
        RedisDataType type = redisConfig.getType();
        RedisDataMode mode = redisConfig.getMode();

        if (type == RedisDataType.STRING) {
            jedis.set(key, concatValues(row));
        } else if (type == RedisDataType.LIST) {
            if (mode == RedisDataMode.L_PUSH) {
                jedis.lpush(key, values);
            } else if (mode == RedisDataMode.R_PUSH) {
                jedis.rpush(key, values);
            }
        } else if (type == RedisDataType.SET) {
            jedis.sadd(key, values);
        } else if (type == RedisDataType.Z_SET) {
            List<Object> scoreValue = getFieldAndValue(row);
            jedis.zadd(key, (Integer) scoreValue.get(0), String.valueOf(scoreValue.get(1)));
        } else if (type == RedisDataType.HASH) {
            key = concatHashKey(row);
            hashWrite(row, key, jedis);
        }

        if (redisConfig.getExpireTime() > 0) {
            if (redisConfig.getExpireTime() > REDIS_CRITICAL_TIME.defaultValue()) {
                jedis.expireAt(key, redisConfig.getExpireTime());
            } else {
                jedis.expire(key, (int) redisConfig.getExpireTime());
            }
        }
        return jedis;
    }

    private void processTimeFormat(ColumnRowData row) {
        for (int i = 0; i < row.getArity(); i++) {
            if (row.getField(i) instanceof TimestampColumn
                    || row.getField(i) instanceof SqlDateColumn) {
                if (StringUtils.isNotBlank(redisConfig.getDateFormat())) {
                    row.setField(i, new StringColumn(sdf.format(row.getField(i).asDate())));
                } else {
                    row.setField(
                            i,
                            new StringColumn(
                                    String.valueOf(row.getField(i).asTimestamp().getTime())));
                }
            }
        }
    }

    private List<Object> getFieldAndValue(ColumnRowData row) {
        if (row.getArity() - redisConfig.getKeyIndexes().size()
                != REDIS_KEY_VALUE_SIZE.defaultValue()) {
            throw new IllegalArgumentException(
                    "Each row record can have only one pair of attributes and values except key");
        }

        List<Object> values = new ArrayList<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            values.add(row.getField(i));
        }
        for (Integer keyIndex : redisConfig.getKeyIndexes()) {
            values.remove((int) keyIndex);
        }

        return values;
    }

    private String[] getValues(ColumnRowData row) {
        List<String> values = new ArrayList<>();

        for (int i = 0; i < row.getArity(); i++) {
            if (!redisConfig.getKeyIndexes().contains(i)) {
                values.add(String.valueOf(row.getField(i)));
            }
        }

        return values.toArray(new String[0]);
    }

    private String concatValues(ColumnRowData row) {
        List<FieldConfig> columns = redisConfig.getColumn();
        Map<String, Object> fieldMap = new HashMap<>();
        int index = 0;

        for (FieldConfig fieldConfig : columns) {
            if (Objects.nonNull(row.getField(index))) {
                fieldMap.put(fieldConfig.getName(), row.getField(index).getData());
            }
            index++;
        }
        return JsonUtil.toJson(fieldMap);
    }

    private String concatKey(ColumnRowData row) {
        if (redisConfig.getKeyIndexes().size() == 1) {
            return String.valueOf(row.getField(redisConfig.getKeyIndexes().get(0)));
        } else {
            List<String> keys = new ArrayList<>(redisConfig.getKeyIndexes().size());
            for (Integer index : redisConfig.getKeyIndexes()) {
                keys.add(String.valueOf(row.getField(index)));
            }
            return StringUtils.join(keys, redisConfig.getKeyFieldDelimiter());
        }
    }

    private String concatHashKey(ColumnRowData row) {
        StringBuilder keyBuilder = new StringBuilder();
        if (CollectionUtils.isNotEmpty(redisConfig.getColumn())) {
            if (StringUtils.isNotBlank(redisConfig.getKeyPrefix())) {
                keyBuilder
                        .append(redisConfig.getKeyPrefix())
                        .append(redisConfig.getKeyFieldDelimiter());
            }
        }
        return keyBuilder.append(concatKey(row)).toString();
    }

    private void hashWrite(ColumnRowData row, String key, Jedis jedis) {
        if (CollectionUtils.isNotEmpty(redisConfig.getColumn())) {
            for (int index : fieldIndex) {
                FieldConfig fieldConfig = redisConfig.getColumn().get(index);
                String field = fieldConfig.getName();
                if (row.getField(index) != null) {
                    jedis.hset(key, field, row.getField(index).asString());
                }
            }
        } else {
            List<Object> fieldValue = getFieldAndValue(row);
            jedis.hset(key, String.valueOf(fieldValue.get(0)), String.valueOf(fieldValue.get(1)));
        }
    }
}
