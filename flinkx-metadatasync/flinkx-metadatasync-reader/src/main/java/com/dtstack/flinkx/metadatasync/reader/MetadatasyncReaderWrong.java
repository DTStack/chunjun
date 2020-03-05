package com.dtstack.flinkx.metadatasync.reader;

import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author : tiezhu
 * @date : 2020/3/3
 * @description : hive元数据全量采集插件
 */
public class MetadatasyncReaderWrong extends JdbcDataReader {

    public MetadatasyncReaderWrong(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        dbUrl = DBUtil.formatJdbcUrl(dbUrl, null);

    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new MetadatasyncInputFormatWrong());
    }

    @Override
    public DataStream<Row> readData() {
        JdbcInputFormatBuilder builder = new JdbcInputFormatBuilder(new MetadatasyncInputFormatWrong());

        builder.setDBUrl(dbUrl);
        builder.setPassword(password);
        builder.setUsername(username);
        builder.setTable(table);
        builder.setDrivername("org.apache.hive.jdbc.HiveDriver");

        return createInput(builder.finish());
    }
}
