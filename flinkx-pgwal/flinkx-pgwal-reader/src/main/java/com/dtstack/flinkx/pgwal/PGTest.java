package com.dtstack.flinkx.pgwal;

import java.sql.*;

public class PGTest {
    public static void main(String[] args) throws SQLException {
        DriverManager.registerDriver(new org.postgresql.Driver());
        String url = "jdbc:postgresql://172.16.101.246:5432/postgres";
        String user = "postgres";
        String password = "abc123";
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement statement = conn.createStatement();
        String sql = "select datname from pg_database";
        ResultSet resultSet = statement.executeQuery(sql);

        System.out.println("databases : ");
        while (resultSet.next()) {
            System.out.print(resultSet.getString("datname") + " ,");
        }
        System.out.println();
        System.out.println();

        System.out.println("tables : ");
        sql = "SELECT table_catalog FROM information_schema.tables WHERE table_schema = 'public';";
        resultSet = statement.executeQuery(sql);
        int index = 0;
        while (resultSet.next()) {
            System.out.print(resultSet.getString("table_catalog") + " ,");
            if(index ++ % 6 == 0) {
                System.out.println();
            }
        }

        System.out.println();
        System.out.println();
        System.out.println("columns : ");
        sql = "SELECT column_name FROM information_schema.columns WHERE table_name ='tables';";
        resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            System.out.print(resultSet.getString("column_name") + " ,");
        }
    }
}
