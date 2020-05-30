package com.dtstack.flinkx.teradata.util;

import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ClassUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    /**
     * 获取表列名类型列表
     * @param dbURL             jdbc url
     * @param username          数据库账号
     * @param password          数据库密码
     * @param databaseInterface DatabaseInterface
     * @param table             表名
     * @param sql       sql
     * @return
     */
    public static List<String> analyzeTable(String dbURL, String username, String password, DatabaseInterface databaseInterface,
                                            String table, String sql) {
        List<String> descColumnTypeList = new ArrayList<>();
        Connection dbConn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            dbConn = getConnection(dbURL, username, password);
            stmt = dbConn.createStatement();
            rs = stmt.executeQuery(databaseInterface.getSqlQuerySqlFields(sql));
            ResultSetMetaData rd = rs.getMetaData();

            for (int i = 1; i <= rd.getColumnCount(); i++) {
                descColumnTypeList.add(rd.getColumnTypeName(i));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            DbUtil.closeDbResources(rs, stmt, dbConn, false);
        }

        return descColumnTypeList;
    }
}
