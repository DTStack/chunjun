package com.dtstack.flinkx.mongodb.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    protected List<String> columnNames;

    protected List<String> columnTypes;

    protected List<String> filterColumns;

    protected List<String> updateColumns;

    protected String mode;

    public MongodbWriter(DataTransferConfig config) {
        super(config);

        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        hostPorts = writerConfig.getParameter().getStringVal(KEY_HOST_PORTS);
        username = writerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = writerConfig.getParameter().getStringVal(KEY_PASSWORD);
        database = writerConfig.getParameter().getStringVal(KEY_DATABASE);
        collection = writerConfig.getParameter().getStringVal(KEY_COLLECTION);
        mode = writerConfig.getParameter().getStringVal(KEY_MODE);
        filterColumns = (List)writerConfig.getParameter().getVal(KEY_FILTER_COLUMN);
        updateColumns = (List)writerConfig.getParameter().getVal(KEY_UPDATE_COLUMN);
        columnNames = new ArrayList<>();
        columnTypes = new ArrayList<>();

        List column = writerConfig.getParameter().getColumn();
        if(column != null && column.size() > 0){
            for (Object colObj : column) {
                if(colObj instanceof Map){
                    Map<String,String> col = (Map<String,String>) colObj;
                    columnNames.add(col.get(KEY_COLUMN_NAME));
                    columnTypes.add(col.get(KEY_COLUMN_TYPE));
                } else if(colObj instanceof String){
                    columnNames.add(String.valueOf(colObj));
                }
            }
        } else {
            throw new IllegalArgumentException("column argument error");
        }
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        MongodbOutputFormatBuilder builder = new MongodbOutputFormatBuilder();

        builder.setHostPorts(hostPorts);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setDatabase(database);
        builder.setCollection(collection);
        builder.setColumnNames(columnNames);
        builder.setColumnTypes(columnTypes);
        builder.setMode(mode);
        builder.setFilterColumns(filterColumns);
        builder.setUpdateColumns(updateColumns);

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
