package com.dtstack.flinkx.teradata.util;

import com.dtstack.flinkx.util.ClassUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author wuhui
 */
public class DBUtil {
    /**
     * 获取数据库连接，不使用DbUtil里的getConnection为了避免Telnet，因为jdbc4与jdbc3不同
     * @param url 连接url
     * @param username 用户名
     * @param password 密码
     * @return 返回connection
     * @throws SQLException 连接失败抛出异常
     */
    public static Connection getConnection(String url, String username, String password) throws SQLException {
        Connection dbConn;
        synchronized (ClassUtil.LOCK_STR){
            DriverManager.setLoginTimeout(10);

            if (username == null) {
                dbConn = DriverManager.getConnection(url);
            } else {
                dbConn = DriverManager.getConnection(url, username, password);
            }
        }

        return dbConn;
    }
}
