package com.dtstack.flinkx.oracle.reader.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by softfly on 18/2/1.
 */
public class OracleLocalTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        final String DRIVER = "oracle.jdbc.OracleDriver";
        final String URL = "jdbc:oracle:thin:dev/pass1234@172.16.8.121:1521:dtstack";
        final String USER = "dev";
        final String PASSWORD = "pass1234";
        Connection connection = null;
        Class.forName(DRIVER);
        connection = DriverManager.getConnection(URL);


    }
}
