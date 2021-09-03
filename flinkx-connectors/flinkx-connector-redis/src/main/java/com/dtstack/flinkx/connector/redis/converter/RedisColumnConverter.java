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

package com.dtstack.flinkx.connector.redis.converter;

import com.dtstack.flinkx.connector.redis.conf.RedisConf;
import com.dtstack.flinkx.connector.redis.enums.RedisDataMode;
import com.dtstack.flinkx.connector.redis.enums.RedisDataType;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static com.dtstack.flinkx.connector.redis.options.RedisOptions.REDIS_CRITICAL_TIME;
import static com.dtstack.flinkx.connector.redis.options.RedisOptions.REDIS_KEY_VALUE_SIZE;

/**
 * @author chuixue
 * @create 2021-06-17 14:32
 * @description
 */
public class RedisColumnConverter extends AbstractRowConverter<Object, Object, Jedis, LogicalType> {

    /** redis Conf */
    private final RedisConf redisConf;
    /** SimpleDateFormat */
    private SimpleDateFormat sdf;

    public RedisColumnConverter(RedisConf redisConf) {
        this.redisConf = redisConf;
        if (StringUtils.isNotBlank(redisConf.getDateFormat())) {
            sdf = new SimpleDateFormat(redisConf.getDateFormat());
        }
    }

    @Override
    public RowData toInternal(Object input) {
        return null;
    }

    @Override
    public Jedis toExternal(RowData rowData, Jedis jedis) {
        ColumnRowData row = (ColumnRowData) rowData;
        processTimeFormat(row);
        String key = concatKey(row);
        String[] values = getValues(row);
        RedisDataType type = redisConf.getType();
        RedisDataMode mode = redisConf.getMode();

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
            List<Object> fieldValue = getFieldAndValue(row);
            jedis.hset(key, String.valueOf(fieldValue.get(0)), String.valueOf(fieldValue.get(1)));
        }

        if (redisConf.getExpireTime() > 0) {
            if (redisConf.getExpireTime() > REDIS_CRITICAL_TIME.defaultValue()) {
                jedis.expireAt(key, redisConf.getExpireTime());
            } else {
                jedis.expire(key, (int) redisConf.getExpireTime());
            }
        }
        return jedis;
    }

    private void processTimeFormat(ColumnRowData row) {
        for (int i = 0; i < row.getArity(); i++) {
            if (row.getField(i) instanceof TimestampColumn) {
                if (StringUtils.isNotBlank(redisConf.getDateFormat())) {
                    row.setField(i, new StringColumn(sdf.format(row.getField(i).asDate())));
                }
            }
        }
    }

    private List<Object> getFieldAndValue(ColumnRowData row) {
        if (row.getArity() - redisConf.getKeyIndexes().size()
                != REDIS_KEY_VALUE_SIZE.defaultValue()) {
            throw new IllegalArgumentException(
                    "Each row record can have only one pair of attributes and values except key");
        }

        List<Object> values = new ArrayList<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            values.add(row.getField(i));
        }
        for (Integer keyIndex : redisConf.getKeyIndexes()) {
            values.remove((int) keyIndex);
        }

        return values;
    }

    private String[] getValues(ColumnRowData row) {
        List<String> values = new ArrayList<>();

        for (int i = 0; i < row.getArity(); i++) {
            if (!redisConf.getKeyIndexes().contains(i)) {
                values.add(String.valueOf(row.getField(i)));
            }
        }

        return values.toArray(new String[values.size()]);
    }

    private String concatValues(ColumnRowData row) {
        return StringUtils.join(getValues(row), redisConf.getValueFieldDelimiter());
    }

    private String concatKey(ColumnRowData row) {
        if (redisConf.getKeyIndexes().size() == 1) {
            return String.valueOf(row.getField(redisConf.getKeyIndexes().get(0)));
        } else {
            List<String> keys = new ArrayList<>(redisConf.getKeyIndexes().size());
            for (Integer index : redisConf.getKeyIndexes()) {
                keys.add(String.valueOf(row.getField(redisConf.getKeyIndexes().get(index))));
            }
            return StringUtils.join(keys, redisConf.getKeyFieldDelimiter());
        }
    }
}
