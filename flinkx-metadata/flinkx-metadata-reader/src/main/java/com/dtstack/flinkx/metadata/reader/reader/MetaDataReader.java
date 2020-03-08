package com.dtstack.flinkx.metadata.reader.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.metadata.reader.inputformat.MetaDataInputFormat;
import com.dtstack.flinkx.metadata.reader.inputformat.MetaDataInputFormatBuilder;
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import com.dtstack.flinkx.metadata.MetaDataCons;

import java.util.List;

/**
 * @author : tiezhu
 * @date : 2020/3/8
 * @description :
 */
public class MetaDataReader extends DataReader {
    protected String dbUrl;
    protected List<String> table;
    protected String username;
    protected String password;
    protected String driverName;

    protected MetaDataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        dbUrl = readerConfig.getParameter().getConnection().get(0).getJdbcUrl().get(0);
        table = readerConfig.getParameter().getConnection().get(0).getTable();
        username = readerConfig.getParameter().getStringVal(MetaDataCons.KEY_CONN_USERNAME);
        password = readerConfig.getParameter().getStringVal(MetaDataCons.KEY_CONN_PASSWORD);
    }

    @Override
    public DataStream<Row> readData() {
        MetaDataInputFormatBuilder builder = getBuilder();

        builder.setDBUrl(dbUrl);
        builder.setPassword(password);
        builder.setUsername(username);
        builder.setTable(table);
        builder.setDriverName(driverName);
        builder.setNumPartitions(1);

        RichInputFormat format = builder.finish();

        return createInput(format);
    }

    protected MetaDataInputFormatBuilder getBuilder(){
        throw new RuntimeException("子类必须覆盖getBuilder方法");
    }
}
