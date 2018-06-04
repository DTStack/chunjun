package com.dtstack.flinkx.postgresql.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.postgresql.PostgresqlDatabaseMeta;
import com.dtstack.flinkx.rdb.datawriter.JdbcDataWriter;

/**
 * @author jiangbo
 * @date 2018/6/4 21:27
 */
public class PostgresqlWriter extends JdbcDataWriter {

    public PostgresqlWriter(DataTransferConfig config) {
        super(config);
        setDatabaseInterface(new PostgresqlDatabaseMeta());
    }
}
