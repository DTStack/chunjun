package com.dtstack.flinkx.mongodb.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.mongodb.Column;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;

import java.util.List;

import static com.dtstack.flinkx.mongodb.MongodbConfigKeys.*;
import static com.dtstack.flinkx.mongodb.MongodbConfigKeys.KEY_COLLECTION;

/**
 * @author jiangbo
 * @date 2018/6/5 21:14
 */
public class MongodbWriter extends DataWriter {

    protected String hostPorts;

    protected String username;

    protected String password;

    protected String database;

    protected String collection;

    protected List<Column> columns;

    protected String replaceKey;

    public MongodbWriter(DataTransferConfig config) {
        super(config);

        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        hostPorts = writerConfig.getParameter().getStringVal(KEY_HOST_PORTS);
        username = writerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = writerConfig.getParameter().getStringVal(KEY_PASSWORD);
        database = writerConfig.getParameter().getStringVal(KEY_DATABASE);
        collection = writerConfig.getParameter().getStringVal(KEY_COLLECTION);
        mode = writerConfig.getParameter().getStringVal(KEY_MODE);
        replaceKey = writerConfig.getParameter().getStringVal(KEY_REPLACE_KEY);
        columns = writerConfig.getParameter().getColumn();
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        MongodbOutputFormatBuilder builder = new MongodbOutputFormatBuilder();

        builder.setHostPorts(hostPorts);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setDatabase(database);
        builder.setCollection(collection);
        builder.setMode(mode);
        builder.setColumns(columns);
        builder.setReplaceKey(replaceKey);

        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);

        OutputFormatSinkFunction formatSinkFunction = new OutputFormatSinkFunction(builder.finish());
        DataStreamSink<?> dataStreamSink = dataSet.addSink(formatSinkFunction);
        dataStreamSink.name("mongodbwriter");

        return dataStreamSink;
    }
}
