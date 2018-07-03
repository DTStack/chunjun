package com.dtstack.flinkx.redis.reader;

import com.dtstack.flinkx.inputformat.RichInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import redis.clients.jedis.JedisCommands;

import java.io.IOException;
import java.util.Properties;

/**
 * @author jiangbo
 * @date 2018/6/6 17:18
 */
public class RedisInputFormat extends RichInputFormat {

    protected Properties properties;

    private JedisCommands jedis;

    private JedisFactory jedisFactory;

    @Override
    public void configure(Configuration parameters) {
        jedisFactory = new JedisFactory(properties);
        jedis = jedisFactory.getJedis();
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {

    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return null;
    }

    @Override
    protected void closeInternal() throws IOException {
        jedisFactory.close(jedis);
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }
}
