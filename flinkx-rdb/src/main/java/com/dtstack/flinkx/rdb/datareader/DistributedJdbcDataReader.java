package com.dtstack.flinkx.rdb.datareader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.inputformat.DistributedJdbcInputFormatBuilder;
import com.dtstack.flinkx.rdb.inputformat.DistributedJdbcInputSplit;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

public class DistributedJdbcDataReader extends DataReader {

    protected DatabaseInterface databaseInterface;

    protected TypeConverterInterface typeConverter;

    protected String username;

    protected String password;

    protected List<String> column;

    protected String where;

    protected String splitKey;

    protected List<DistributedJdbcInputSplit.DataSource> sourceList;

    protected DistributedJdbcDataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        username = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_USER_NAME);
        password = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_PASSWORD);
        where = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_WHERE);
        column = readerConfig.getParameter().getColumn();
        splitKey = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_SPLIK_KEY);

        for (ReaderConfig.ParameterConfig.ConnectionConfig connectionConfig : readerConfig.getParameter().getConnection()) {
            String curUsername = (connectionConfig.getUsername() == null || connectionConfig.getUsername().length() == 0)
                    ? username : connectionConfig.getUsername();
            String curPassword = (connectionConfig.getPassword() == null || connectionConfig.getPassword().length() == 0)
                    ? password : connectionConfig.getPassword();
            String curJdbcUrl = connectionConfig.getJdbcUrl().get(0);
            for (String table : connectionConfig.getTable()) {
                DistributedJdbcInputSplit.DataSource source = new DistributedJdbcInputSplit.DataSource();
                source.setTable(table);
                source.setUserName(curUsername);
                source.setPassword(curPassword);
                source.setJdbcUrl(curJdbcUrl);

                sourceList.add(source);
            }
        }
    }

    @Override
    public DataStream<Row> readData() {
        DistributedJdbcInputFormatBuilder builder = new DistributedJdbcInputFormatBuilder();
        builder.setDrivername(databaseInterface.getDriverClass());
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);
        builder.setDatabaseInterface(databaseInterface);
        builder.setTypeConverter(typeConverter);
        builder.setColumn(column);
        builder.setSourceList(sourceList);
        builder.setNumPartitions(numPartitions);
        builder.setSplitKey(splitKey);

        RichInputFormat format =  builder.finish();
        return createInput(format, (databaseInterface.getDatabaseType() + "dreader").toLowerCase());
    }

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        this.databaseInterface = databaseInterface;
    }
}
