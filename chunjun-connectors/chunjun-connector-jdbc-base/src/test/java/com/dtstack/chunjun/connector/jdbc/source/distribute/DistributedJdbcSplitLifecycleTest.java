/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.jdbc.source.distribute;

import com.dtstack.chunjun.connector.jdbc.config.DataSourceConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DistributedJdbcSplitLifecycleTest {

    @Test
    public void readsNonEmptySplitAfterEmptySplit() throws Exception {
        verifySplits(
                Collections.emptyList(),
                Collections.singletonList(source("first_table")),
                Collections.emptyList(),
                Collections.singletonList(1));
    }

    @Test
    public void readsSingleSourceSplitAfterMultipleSources() throws Exception {
        verifySplits(
                Arrays.asList(source("first_table"), source("second_table")),
                Collections.singletonList(source("third_table")),
                Arrays.asList(1, 2),
                Collections.singletonList(3));
    }

    private void verifySplits(
            List<DataSourceConfig> first,
            List<DataSourceConfig> second,
            List<Integer> expectedFirst,
            List<Integer> expectedSecond)
            throws Exception {
        String url = "jdbc:h2:mem:" + UUID.randomUUID();
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL(url);
        try (Connection anchor = dataSource.getConnection();
                Statement statement = anchor.createStatement()) {
            statement.execute("CREATE TABLE first_table (id INT)");
            statement.execute("CREATE TABLE second_table (id INT)");
            statement.execute("CREATE TABLE third_table (id INT)");
            statement.execute("INSERT INTO first_table VALUES (1)");
            statement.execute("INSERT INTO second_table VALUES (2)");
            statement.execute("INSERT INTO third_table VALUES (3)");
            TestInputFormat format = new TestInputFormat(dataSource);
            try {
                format.openInternal(split(0, first));
                Assert.assertEquals(expectedFirst, readAll(format));
                format.closeInternal();
                format.openInternal(split(1, second));
                Assert.assertEquals(expectedSecond, readAll(format));
            } finally {
                format.closeInternal();
            }
            for (Connection connection : format.connections) {
                Assert.assertTrue("Split connections must be closed", connection.isClosed());
            }
        }
    }

    private static List<Integer> readAll(TestInputFormat format) throws Exception {
        List<Integer> values = new ArrayList<>();
        while (!format.reachedEnd()) {
            values.add(format.nextRecordInternal(null).getInt(0));
        }
        return values;
    }

    private static DistributedJdbcInputSplit split(int number, List<DataSourceConfig> sources) {
        return new DistributedJdbcInputSplit(number, 2, sources, "mod", false);
    }

    private static DataSourceConfig source(String table) {
        DataSourceConfig config = new DataSourceConfig();
        config.setTable(table);
        return config;
    }

    private static class TestInputFormat extends DistributedJdbcInputFormat {
        private final DataSource dataSource;
        private final List<Connection> connections = new ArrayList<>();

        TestInputFormat(DataSource dataSource) throws Exception {
            this.dataSource = dataSource;
            jdbcConfig = new JdbcConfig();
            rowConverter = mock(AbstractRowConverter.class);
            when(rowConverter.toInternal(any()))
                    .thenAnswer(
                            invocation ->
                                    GenericRowData.of(
                                            ((ResultSet) invocation.getArgument(0)).getInt(1)));
        }

        @Override
        protected Connection getConnection() throws SQLException {
            Connection connection = dataSource.getConnection();
            connections.add(connection);
            return connection;
        }

        @Override
        protected String buildQuerySql(InputSplit split) {
            return "SELECT id FROM " + sourceList.get(sourceIndex).getTable();
        }
    }
}
