package com.dtstack.flinkx.mongodb.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.mongodb.MongodbConfigKeys.*;

/**
 * @author jiangbo
 * @date 2018/6/5 10:20
 */
public class MongodbReader extends DataReader {

    protected String hostPorts;

    protected String username;

    protected String password;

    protected String database;

    protected String collection;

    protected List<String> columnNames;

    protected List<String> columnTypes;

    protected Map filter;

    public MongodbReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        hostPorts = readerConfig.getParameter().getStringVal(KEY_HOST_PORTS);
        username = readerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = readerConfig.getParameter().getStringVal(KEY_PASSWORD);
        database = readerConfig.getParameter().getStringVal(KEY_DATABASE);
        collection = readerConfig.getParameter().getStringVal(KEY_COLLECTION);
        filter = (Map)readerConfig.getParameter().getVal(KEY_FILTER);
        columnNames = new ArrayList<>();
        columnTypes = new ArrayList<>();

        List column = readerConfig.getParameter().getColumn();
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
    public DataStream<Row> readData() {
        MongodbInputFormatBuilder builder = new MongodbInputFormatBuilder();

        builder.setHostPorts(hostPorts);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setDatabase(database);
        builder.setCollection(collection);
        builder.setColumnNames(columnNames);
        builder.setColumnTypes(columnTypes);
        builder.setFilter(filter);

        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);

        return createInput(builder.finish(),"mongodbreader");
    }
}
