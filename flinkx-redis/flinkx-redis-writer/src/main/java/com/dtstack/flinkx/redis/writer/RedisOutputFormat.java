package com.dtstack.flinkx.redis.writer;

import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.RichOutputFormat;
import com.dtstack.flinkx.redis.DataMode;
import com.dtstack.flinkx.redis.DataType;
import com.dtstack.flinkx.redis.JedisUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.*;

import static com.dtstack.flinkx.redis.RedisConfigKeys.*;

/**
 * @author jiangbo
 * @date 2018/7/3 15:52
 */
public class RedisOutputFormat extends RichOutputFormat {

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

    private static final int CRITICAL_TIME = 60 * 60 * 24 * 30;

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        Properties properties = new Properties();
        properties.put(KEY_HOST_PORT,hostPort);
        properties.put(KEY_PASSWORD,password);
        properties.put(KEY_TIMEOUT,timeout);
        properties.put(KEY_DB,database);

        jedis = JedisUtil.getJedis(properties);
    }

    @Override
    protected void openInternal(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    protected void writeSingleRecordInternal(Row row) throws WriteRecordException {
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

        if (expireTime > CRITICAL_TIME){
            jedis.expireAt(key,expireTime);
        } else {
            jedis.expire(key,(int)expireTime);
        }
    }

    private List<Object> getFieldAndValue(Row row){
        if(row.getArity() - keyIndexes.size() != 2){
            throw new IllegalArgumentException("Each row record can have only one pair of attributes and values except key");
        }

        List<Object> values = new ArrayList<>(row.getArity());
        for (int i = 0; i < row.getArity(); i++) {
            values.add(row.getField(i));
        }

        keyIndexes.forEach(i -> values.remove(i));

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
        // Still not supported
    }

    @Override
    public void closeInternal() throws IOException {
        super.closeInternal();
        JedisUtil.close(jedis);
    }
}
