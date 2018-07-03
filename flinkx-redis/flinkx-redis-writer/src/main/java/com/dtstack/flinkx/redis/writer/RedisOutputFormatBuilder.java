package com.dtstack.flinkx.redis.writer;


import com.dtstack.flinkx.outputformat.RichOutputFormatBuilder;
import com.dtstack.flinkx.redis.DataMode;
import com.dtstack.flinkx.redis.DataType;

import java.util.List;

/**
 * @author jiangbo
 * @date 2018/7/3 15:52
 */
public class RedisOutputFormatBuilder extends RichOutputFormatBuilder {

    private RedisOutputFormat format;

    public RedisOutputFormatBuilder() {
        super.format = format = new RedisOutputFormat();
    }

    public void setHostPort(String hostPort) {
        this.format.hostPort = hostPort;
    }

    public void setPassword(String password) {
        this.format.password = password;
    }

    public void setDatabase(int database) {
        this.format.database = database;
    }

    public void setKeyIndexes(List<Integer> keyIndexes) {
        this.format.keyIndexes = keyIndexes;
    }

    public void setKeyFieldDelimiter(String keyFieldDelimiter) {
        this.format.keyFieldDelimiter = keyFieldDelimiter;
    }

    public void setDateFormat(String dateFormat) {
        this.format.dateFormat = dateFormat;
    }

    public void setExpireTime(long expireTime) {
        this.format.expireTime = expireTime;
    }

    public void setTimeout(int timeout) {
        this.format.timeout = timeout;
    }

    public void setType(DataType type) {
        this.format.type = type;
    }

    public void setDataMode(DataMode dataMode) {
        this.format.dataMode = dataMode;
    }

    public void setValueFieldDelimiter(String valueFieldDelimiter) {
        this.format.valueFieldDelimiter = valueFieldDelimiter;
    }

    @Override
    protected void checkFormat() {
        if(format.hostPort == null){
            throw new IllegalArgumentException("No host and port supplied");
        }

        if (format.keyIndexes == null || format.keyIndexes.size() == 0){
            throw new IllegalArgumentException("Field keyIndexes cannot be empty");
        }
    }
}
