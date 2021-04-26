/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
