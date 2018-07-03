package com.dtstack.flinkx.redis.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

import static com.dtstack.flinkx.redis.RedisConfigKeys.*;

/**
 * @author jiangbo
 * @date 2018/6/6 17:17
 */
public class RedisReader extends DataReader {

    protected Properties properties;

    public RedisReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        properties.setProperty(KEY_HOST,readerConfig.getStringVal(KEY_HOST));
        properties.setProperty(KEY_PORT,readerConfig.getStringVal(KEY_PORT));
        properties.setProperty(KEY_PASSWORD,readerConfig.getStringVal(KEY_PASSWORD));
        properties.setProperty(KEY_DB,readerConfig.getStringVal(KEY_DB));
        properties.setProperty(KEY_MODEL,readerConfig.getStringVal(KEY_MODEL));
        properties.setProperty(KEY_HOST_PORT_LIST,readerConfig.getStringVal(KEY_HOST_PORT_LIST));
    }

    @Override
    public DataStream<Row> readData() {
        RedisInputFormatBuilder builder = new RedisInputFormatBuilder();
        builder.setProperties(properties);

        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);

        return createInput(builder.finish(),"redisreader");
    }
}
