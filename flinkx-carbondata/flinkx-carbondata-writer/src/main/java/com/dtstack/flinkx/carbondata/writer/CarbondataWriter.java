package com.dtstack.flinkx.carbondata.writer;

import com.dtstack.flinkx.carbondata.CarbondataDatabaseMeta;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.datawriter.JdbcDataWriter;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.writer.DataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.*;
import static com.dtstack.flinkx.rdb.datawriter.JdbcConfigKeys.KEY_FULL_COLUMN;

/**
 * Carbondata writer plugin
 *
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */
public class CarbondataWriter extends DataWriter {

    protected DatabaseInterface databaseInterface;
    protected String dbUrl;
    protected String username;
    protected String password;
    protected List<String> column;
    protected String table;
    protected int batchSize;
    protected List<String> preSql;
    protected List<String> postSql;


    private static final int DEFAULT_BATCH_SIZE = 1024;

    public CarbondataWriter(DataTransferConfig config) {
        super(config);
        setDatabaseInterface(new CarbondataDatabaseMeta());
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        dbUrl = writerConfig.getParameter().getConnection().get(0).getJdbcUrl();
        username = writerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = writerConfig.getParameter().getStringVal(KEY_PASSWORD);
        table = writerConfig.getParameter().getConnection().get(0).getTable().get(0);
        batchSize = writerConfig.getParameter().getIntVal(KEY_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        column = (List<String>) writerConfig.getParameter().getColumn();
        preSql = (List<String>) writerConfig.getParameter().getVal(KEY_PRE_SQL);
        postSql = (List<String>) writerConfig.getParameter().getVal(KEY_POST_SQL);

    }

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        this.databaseInterface = databaseInterface;
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        CarbondataOutputFormatBuilder builder = new CarbondataOutputFormatBuilder();
        builder.setDBUrl(dbUrl);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setBatchInterval(batchSize);
        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setErrorRatio(errorRatio);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        builder.setTable(table);
        builder.setColumn(column);
        builder.setPreSql(preSql);
        builder.setPostSql(postSql);

        OutputFormatSinkFunction sinkFunction = new OutputFormatSinkFunction(builder.finish());
        DataStreamSink<?> dataStreamSink = dataSet.addSink(sinkFunction);
        String sinkName = "carbondatawriter";
        dataStreamSink.name(sinkName);
        return dataStreamSink;
    }

}

