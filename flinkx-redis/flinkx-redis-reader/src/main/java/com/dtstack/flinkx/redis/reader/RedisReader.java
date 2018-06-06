package com.dtstack.flinkx.redis.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author jiangbo
 * @date 2018/6/6 17:17
 */
public class RedisReader extends DataReader {

    public RedisReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

    }

    @Override
    public DataStream<Row> readData() {
        return null;
    }
}
