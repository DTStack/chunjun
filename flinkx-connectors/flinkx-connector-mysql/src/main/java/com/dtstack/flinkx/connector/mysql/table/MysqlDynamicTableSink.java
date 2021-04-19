package com.dtstack.flinkx.connector.mysql.table;

import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.outputformat.JdbcOutputFormat;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcDynamicTableSink;

import com.dtstack.flinkx.connector.mysql.outputFormat.MysqlOutputFormat;

import org.apache.flink.table.api.TableSchema;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/04/18
 **/
public class MysqlDynamicTableSink extends JdbcDynamicTableSink {

    public MysqlDynamicTableSink(
            JdbcConf jdbcConf,
            JdbcDialect jdbcDialect,
            TableSchema tableSchema) {
        super(jdbcConf, jdbcDialect, tableSchema);
    }

    @Override
    protected JdbcOutputFormat getOutputFormat() {
        return new MysqlOutputFormat();
    }
}

