package com.dtstack.flinkx.redis.reader;

import com.dtstack.flinkx.inputformat.RichInputFormatBuilder;

import java.util.Properties;

/**
 * @author jiangbo
 * @date 2018/6/6 17:19
 */
public class RedisInputFormatBuilder extends RichInputFormatBuilder {

    private RedisInputFormat format;

    public void setProperties(Properties properties) {
        format.properties = properties;
    }

    @Override
    protected void checkFormat() {

    }
}
