package com.dtstack.flinkx.restore.mysql.utils;

import com.alibaba.druid.pool.DruidDataSource;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/8 星期三
 */
public class DruidDataSourceUtil {

    private DruidDataSourceUtil() {}

    public static final String JDBC_URL_KEY = "jdbc.url";

    public static final String USER_NAME_KEY = "username";

    public static final String PASSWORD_KEY = "password";

    public static final String TABLE_KEY = "table";

    public static final String DATABASE_KEY = "database";

    /**
     * 校验参数的合法性
     *
     * @param properties 参数
     */
    private static void checkLegitimacy(Map<String, Object> properties) {
        if (null == properties) {
            throw new NullPointerException("Get null properties for store");
        }

        StringBuilder errorMessageBuilder = new StringBuilder();

        if (null == properties.get(JDBC_URL_KEY)) {
            errorMessageBuilder.append("No jdbc.url supplied!\n");
        }

        if (null == properties.get(USER_NAME_KEY)) {
            errorMessageBuilder.append("No username supplied!\n");
        }

        if (null == properties.get(TABLE_KEY)) {
            errorMessageBuilder.append("No table supplied!\n");
        }

        if (null == properties.get(DATABASE_KEY)) {
            errorMessageBuilder.append("No database supplied!\n");
        }

        if (errorMessageBuilder.length() > 0) {
            throw new IllegalArgumentException(errorMessageBuilder.toString());
        }
    }

    public static DataSource getDataSource(Map<String, Object> conf, String driverName)
            throws SQLException, ClassNotFoundException {

        checkLegitimacy(conf);

        Class.forName(driverName);

        String jdbcUrl = (String) conf.get(JDBC_URL_KEY);
        String username = (String) conf.get(USER_NAME_KEY);
        String password = conf.get(PASSWORD_KEY) == null ? null : (String) conf.get(PASSWORD_KEY);
        String database = (String) conf.get(DATABASE_KEY);

        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driverName);
        dataSource.setName(database + "-druid.source");

        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(5);
        dataSource.setMaxWait(30000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setValidationQuery("select 'x'");
        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnBorrow(false);
        dataSource.setTestOnReturn(false);
        dataSource.setKeepAlive(true);
        dataSource.setPoolPreparedStatements(false);
        dataSource.setConnectionInitSqls(Collections.singletonList("set names 'utf8'"));

        dataSource.setRemoveAbandoned(true);
        dataSource.setRemoveAbandonedTimeout(600);
        dataSource.setLogAbandoned(true);
        //        dataSource.setBreakAfterAcquireFailure(true);
        dataSource.setTimeBetweenConnectErrorMillis(60000);
        dataSource.setConnectionErrorRetryAttempts(3);

        dataSource.init();
        return dataSource;
    }
}
