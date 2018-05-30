package com.dtstack.flinkx.postgresql.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.postgresql.PostgresqlDatabaseMeta;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jiangbo
 * @date 2018/5/25 11:19
 */
public class PostgresqlReader extends JdbcDataReader {

    public PostgresqlReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new PostgresqlDatabaseMeta());
    }

}
