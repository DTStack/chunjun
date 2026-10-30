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
package com.dtstack.chunjun.connector.jdbc.util;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.config.SourceConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.util.key.NumericTypeUtil;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.io.GenericInputSplit;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.Assert;
import org.junit.Test;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

public class CustomSqlWrapperTest {
    private final JdbcDialect dialect = mock(JdbcDialect.class, CALLS_REAL_METHODS);

    @Test
    public void readsQueryWithTrailingLineComment() throws Exception {
        verifyWrappers("SELECT \"id\" FROM source_table -- trailing comment");
    }

    @Test
    public void readsQueryWithTrailingSemicolonAndComment() throws Exception {
        verifyWrappers("SELECT \"id\" FROM source_table; -- trailing comment");
    }

    @Test
    public void preservesQuotedSemicolonsAndCommentMarkers() throws Exception {
        verifyWrappers("SELECT \"id\" FROM source_table WHERE \"name\" = 'a;--b'; /* tail */");
    }

    private void verifyWrappers(String customSql) throws Exception {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID());
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE source_table (\"id\" INT, \"name\" VARCHAR(50))");
            statement.execute("INSERT INTO source_table VALUES (7, 'a;--b')");

            String[] columns = {"id"};
            String select =
                    dialect.getSelectFromStatement(null, "source_table", customSql, columns, null);
            String projected =
                    dialect.getSelectFromStatement(
                            null, "source_table", customSql, columns, new String[0], null);
            for (String query : Arrays.asList(select, projected)) {
                try (ResultSet resultSet = statement.executeQuery(query)) {
                    Assert.assertTrue(resultSet.next());
                    Assert.assertEquals(7, resultSet.getInt(1));
                    Assert.assertFalse(resultSet.next());
                }
            }
            String metadata =
                    dialect.getMetadataQuerySql(
                            Collections.singletonList("id"), customSql, "source_table");
            try (ResultSet resultSet = statement.executeQuery(metadata)) {
                Assert.assertEquals(1, resultSet.getMetaData().getColumnCount());
                Assert.assertFalse(resultSet.next());
            }
            JdbcConfig config = new JdbcConfig();
            config.setCustomSql(customSql);
            config.setSplitPk("id");
            try (ResultSet resultSet =
                    statement.executeQuery(SqlUtil.buildQuerySplitRangeSql(config, dialect))) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(7, resultSet.getInt("min_value"));
                Assert.assertEquals(7, resultSet.getInt("max_value"));
            }
            Assert.assertEquals("7", new MaxValueInputFormat(dataSource, customSql).maximum());
        }
    }

    private class MaxValueInputFormat extends JdbcInputFormat {
        private final DataSource dataSource;

        MaxValueInputFormat(DataSource dataSource, String customSql) {
            this.dataSource = dataSource;
            jdbcConfig = new JdbcConfig();
            SourceConnectionConfig connection = new SourceConnectionConfig();
            connection.setTable(Collections.singletonList("source_table"));
            jdbcConfig.setConnection(Collections.singletonList(connection));
            jdbcConfig.setCustomSql(customSql);
            jdbcConfig.setIncreColumn("id");
            jdbcDialect = dialect;
            incrementKeyUtil = new NumericTypeUtil();
            setRuntimeContext(mock(RuntimeContext.class));
        }

        String maximum() {
            return getMaxValue(new GenericInputSplit(0, 1));
        }

        @Override
        protected Connection getConnection() throws SQLException {
            return dataSource.getConnection();
        }
    }
}
