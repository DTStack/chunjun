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

package com.dtstack.flinkx.redis.writer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.redis.DataMode;
import com.dtstack.flinkx.redis.DataType;
import com.dtstack.flinkx.redis.JedisUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static com.dtstack.flinkx.redis.RedisConfigKeys.KEY_DB;
import static com.dtstack.flinkx.redis.RedisConfigKeys.KEY_HOST_PORT;
import static com.dtstack.flinkx.redis.RedisConfigKeys.KEY_PASSWORD;
import static com.dtstack.flinkx.redis.RedisConfigKeys.KEY_TIMEOUT;

/**
 * OutputFormat for writing data to redis database.
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class RedisOutputFormat extends BaseRichOutputFormat {

    protected String hostPort;

    protected String password;

    protected int database;

    protected List<Integer> keyIndexes;

    protected String keyFieldDelimiter;

    protected String dateFormat;

    protected long expireTime;

    protected int timeout;

    protected DataType type;

    protected DataMode dataMode;

    protected String valueFieldDelimiter;

    private Jedis jedis;

    private SimpleDateFormat sdf;

    private static final int CRITICAL_TIME = 60 * 60 * 24 * 30;

    private static final int KEY_VALUE_SIZE = 2;

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        Properties properties = new Properties();
        properties.put(KEY_HOST_PORT,hostPort);
        if(StringUtils.isNotBlank(password)){
            properties.put(KEY_PASSWORD,password);
        }
        properties.put(KEY_TIMEOUT,timeout);
        properties.put(KEY_DB,database);

        jedis = JedisUtil.getJedis(properties);

        if (StringUtils.isNotBlank(dateFormat)){
            sdf = new SimpleDateFormat(dateFormat);
        }
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
        processTimeFormat(row);
        String key = concatKey(row);
        String[] values = getValues(row);

        if(type == DataType.STRING){
            jedis.set(key,concatValues(row));
        } else if(type == DataType.LIST){
            if(dataMode == DataMode.L_PUSH){
                jedis.lpush(key,values);
            } else if(dataMode == DataMode.R_PUSH){
                jedis.rpush(key,values);
            }
        } else if(type == DataType.SET){
            jedis.sadd(key,values);
        } else if(type == DataType.Z_SET){
            List<Object> scoreValue = getFieldAndValue(row);
            jedis.zadd(key,(Integer)scoreValue.get(0),String.valueOf(scoreValue.get(1)));
        } else if(type == DataType.HASH){
            List<Object> fieldValue = getFieldAndValue(row);
            jedis.hset(key,String.valueOf(fieldValue.get(0)),String.valueOf(fieldValue.get(1)));
        }

        if(expireTime > 0){
            if (expireTime > CRITICAL_TIME){
                jedis.expireAt(key,expireTime);
            } else {
                jedis.expire(key,(int)expireTime);
            }
        }
    }

    private void processTimeFormat(Row row){
        for (int i = 0; i < row.getArity(); i++) {
            if(row.getField(i) instanceof Date){
                if (StringUtils.isNotBlank(dateFormat)){
                    row.setField(i,sdf.format((Date)row.getField(i)));
                }else {
                    row.setField(i,((Date)row.getField(i)).getTime());
                }
            }
        }
    }

    private List<Object> getFieldAndValue(Row row){
        if(row.getArity() - keyIndexes.size() != KEY_VALUE_SIZE){
            throw new IllegalArgumentException("Each row record can have only one pair of attributes and values except key");
        }

        List<Object> values = new ArrayList<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            values.add(row.getField(i));
        }

        for (Integer keyIndex : keyIndexes) {
            values.remove((int)keyIndex);
        }

        return values;
    }

    private String[] getValues(Row row){
        List<String> values = new ArrayList<>();

        for (int i = 0; i < row.getArity(); i++) {
            if(!keyIndexes.contains(i)){
                values.add(String.valueOf(row.getField(i)));
            }
        }

        return values.toArray(new String[values.size()]);
    }

    private String concatValues(Row row){
        return StringUtils.join(getValues(row),valueFieldDelimiter);
    }

    private String concatKey(Row row){
        if (keyIndexes.size() == 1){
            return String.valueOf(row.getField(keyIndexes.get(0)));
        } else {
            List<String> keys = new ArrayList<>(keyIndexes.size());
            for (Integer index : keyIndexes) {
                keys.add(String.valueOf(row.getField(keyIndexes.get(index))));
            }

            return StringUtils.join(keys,keyFieldDelimiter);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() throws Exception {
        notSupportBatchWrite("RedisWriter");
    }

    @Override
    public void closeInternal() throws IOException {
        super.closeInternal();
        JedisUtil.close(jedis);
    }
}
