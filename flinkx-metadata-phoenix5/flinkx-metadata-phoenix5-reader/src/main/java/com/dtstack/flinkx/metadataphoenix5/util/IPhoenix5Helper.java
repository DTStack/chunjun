package com.dtstack.flinkx.metadataphoenix5.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Date: 2020/10/10
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public interface IPhoenix5Helper {

    String CLASS_STR = "public transient RowProjector rowProjector;\n" +
            "    public List<PDataType> instanceList;\n" +
            "\n" +
            "    @Override\n" +
            "    public Connection getConn(String url, Properties properties) throws SQLException {\n" +
            "        Connection dbConn;\n" +
            "        synchronized (ClassUtil.LOCK_STR) {\n" +
            "            DriverManager.setLoginTimeout(10);\n" +
            "            // telnet\n" +
            "            TelnetUtil.telnet(url);\n" +
            "            dbConn = DriverManager.getConnection(url, properties);\n" +
            "        }\n" +
            "\n" +
            "        return dbConn;\n" +
            "    }\n" +
            "\n";

    /**
     * 获取phoenix jdbc连接
     * @param url
     * @param properties
     * @return
     * @throws SQLException
     */
    Connection getConn(String url, Properties properties) throws SQLException;

}
