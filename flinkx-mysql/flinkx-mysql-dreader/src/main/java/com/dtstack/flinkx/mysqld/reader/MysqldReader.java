package com.dtstack.flinkx.mysqld.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.mysql.MySqlDatabaseMeta;
import com.dtstack.flinkx.rdb.datareader.DistributedJdbcDataReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MysqldReader extends DistributedJdbcDataReader {

    public MysqldReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new MySqlDatabaseMeta());
    }
}
