package com.dtstack.flinkx.mongodb.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

/**
 * @author jiangbo
 * @date 2018/6/5 21:14
 */
public class MongodbWriter extends DataWriter {

    public MongodbWriter(DataTransferConfig config) {
        super(config);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        // TODO

        return null;
    }
}
