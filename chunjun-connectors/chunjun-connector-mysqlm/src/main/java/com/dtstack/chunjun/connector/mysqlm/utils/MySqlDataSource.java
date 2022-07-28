package com.dtstack.chunjun.connector.mysqlm.utils;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.Collections;

public class MySqlDataSource {

    private static final String DRIVER = "com.mysql.jdbc.Driver";

    public static DataSource getDataSource(String jdbcUrl, String username, String password)
            throws SQLException {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(DRIVER);
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(5);
        dataSource.setMaxWait(30000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);
        dataSource.setKeepAlive(true);
        dataSource.setPoolPreparedStatements(false);
        dataSource.setConnectionInitSqls(Collections.singletonList("set names 'utf8'"));

        dataSource.setRemoveAbandoned(false);
        dataSource.setLogAbandoned(true);
        dataSource.setTimeBetweenConnectErrorMillis(60000);
        dataSource.setConnectionErrorRetryAttempts(3);
        dataSource.init();

        return dataSource;
    }
}
