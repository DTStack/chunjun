package com.dtstack.flinkx.connector.clickhouse.util;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ClickhouseUtilTest {

    @Test
    public void getConnection() throws SQLException {
        //        String url =
        // "jdbc:clickhouse://192.168.13.36:8123,192.168.13.37:8123,192.168.13.38:8123/stock";
        //        String password = "cktest$R";
        String username = "default";

        String url = "jdbc:clickhouse://127.0.0.1:8123,127.0.0.1:18123/stock";
        String password = "";

        Connection connection = ClickhouseUtil.getConnection(url, username, password);
        while (true) {
            try {
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("show tables");
                System.out.println(resultSet);
                Assert.assertNotNull(resultSet);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {

                }
            }
        }
    }
}
