package com.dtstack.flinkx.connector.jdbc;

import com.dtstack.flinkx.util.ClassUtil;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @program: flinkx-test
 * @author: wuren
 * @create: 2021/04/08
 **/
public class DtJdbcOutputFormat extends RichOutputFormat {

    protected String username;

    protected String password;

    protected String driverName;

    protected String dbUrl;

    protected Connection dbConn;

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks)  {

    }

    @Override
    public void writeRecord(Object record) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
