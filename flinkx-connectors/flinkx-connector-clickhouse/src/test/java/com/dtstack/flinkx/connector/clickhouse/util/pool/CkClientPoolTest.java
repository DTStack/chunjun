package com.dtstack.flinkx.connector.clickhouse.util.pool;

import com.dtstack.flinkx.connector.clickhouse.util.CkProperties;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class CkClientPoolTest {

    @Test
    public void testClickhouseQuery() throws Exception {
        CkClientFactory clientFactory = new CkClientFactory();
        CkProperties ckProperties = new CkProperties();
        String url = "jdbc:clickhouse://127.0.0.1:8123,127.0.0.1:18123";
        String password = "";

        ckProperties.setUrl(url);
        ckProperties.setPassword(password);
        clientFactory.setCkProperties(ckProperties);
        CkClientPool clientPool = new CkClientPool(clientFactory);

        while (true) {
            Connection conn = clientPool.borrowObject();
            try {
                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery("show tables");
                System.out.println(resultSet);
            } finally {
                clientPool.returnObject(conn);
            }
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
