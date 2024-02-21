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

package com.dtstack.chunjun.connector.jdbc.source.distribute;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.chunjun.connector.jdbc.config.ConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.config.DataSourceConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.config.SourceConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcRawTypeConverterTest;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.exclusion.FieldNameExclusionStrategy;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.ColumnBuildUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.RangeSplitUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.core.io.InputSplit;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.dtstack.chunjun.connector.jdbc.util.JdbcUtilTest.readFile;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.powermock.api.mockito.PowerMockito.doCallRealMethod;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.reflect.Whitebox.setInternalState;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    DistributedJdbcInputFormat.class,
    DistributedJdbcInputSplit.class,
    RangeSplitUtil.class,
    Logger.class,
    TableUtil.class,
    JdbcUtil.class,
    ColumnBuildUtil.class,
    JdbcDialect.class
})
public class DistributeJdbcInputFormatTest {

    private static DistributedJdbcInputFormat inputFormat;
    private static DistributedJdbcInputSplit inputSplit;
    private static List<DataSourceConfig> dataSourceConfigList;
    private static SyncConfig syncConfig;
    private static JdbcConfig jdbcConfig;
    private static JdbcDialect jdbcDialect;

    @Before
    public void setup() throws IOException {
        mockStatic(RangeSplitUtil.class);
        mockStatic(TableUtil.class);
        mockStatic(ColumnBuildUtil.class);
        mockStatic(JdbcUtil.class);

        inputFormat = mock(DistributedJdbcInputFormat.class);
        inputSplit = mock(DistributedJdbcInputSplit.class);
        jdbcConfig = mock(JdbcConfig.class);
        jdbcDialect = mock(JdbcDialect.class);

        setInternalState(inputFormat, "jdbcConfig", jdbcConfig);
        setInternalState(inputFormat, "jdbcDialect", jdbcDialect);

        String json = readFile("distribute_sync_test.json");
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(
                                ConnectionConfig.class,
                                new ConnectionAdapter(SourceConnectionConfig.class.getName()))
                        .addDeserializationExclusionStrategy(
                                new FieldNameExclusionStrategy("column"))
                        .create();
        GsonUtil.setTypeAdapter(gson);
        syncConfig = SyncConfig.parseJob(json);
        JdbcConfig jdbcConfig =
                gson.fromJson(gson.toJson(syncConfig.getReader().getParameter()), JdbcConfig.class);
        List<ConnectionConfig> connectionConfigList = jdbcConfig.getConnection();
        dataSourceConfigList = new ArrayList<>(connectionConfigList.size());
        for (ConnectionConfig connectionConfig : connectionConfigList) {
            String currentUsername =
                    (StringUtils.isNotBlank(connectionConfig.getUsername()))
                            ? connectionConfig.getUsername()
                            : jdbcConfig.getUsername();
            String currentPassword =
                    (StringUtils.isNotBlank(connectionConfig.getPassword()))
                            ? connectionConfig.getPassword()
                            : jdbcConfig.getPassword();

            String schema = connectionConfig.getSchema();
            for (String table : connectionConfig.getTable()) {
                DataSourceConfig dataSourceConfig = new DataSourceConfig();
                dataSourceConfig.setUserName(currentUsername);
                dataSourceConfig.setPassword(currentPassword);
                dataSourceConfig.setJdbcUrl(connectionConfig.obtainJdbcUrl());
                dataSourceConfig.setTable(table);
                dataSourceConfig.setSchema(schema);

                dataSourceConfigList.add(dataSourceConfig);
            }
        }
    }

    @Test
    public void createInputSplitsInternalTest() {
        when(inputFormat.createInputSplitsInternal(2)).thenCallRealMethod();

        when(jdbcConfig.getParallelism()).thenReturn(1);
        Assert.assertThrows(
                ChunJunRuntimeException.class, () -> inputFormat.createInputSplitsInternal(2));

        when(jdbcConfig.getParallelism()).thenReturn(2);
        setInternalState(inputFormat, "sourceList", dataSourceConfigList);
        when(RangeSplitUtil.subListBySegment(dataSourceConfigList, 2)).thenCallRealMethod();
        InputSplit[] inputSplitsInternal = inputFormat.createInputSplitsInternal(2);
        Assert.assertEquals(2, inputSplitsInternal.length);
    }

    @Test
    public void openInternalTest() {
        doCallRealMethod().when(inputFormat).openInternal(inputSplit);
        when(inputSplit.getSourceList()).thenReturn(dataSourceConfigList);

        when(jdbcConfig.getColumn()).thenReturn(syncConfig.getReader().getFieldList());
        setInternalState(inputFormat, "columnTypeList", new ArrayList<>());
        setInternalState(inputFormat, "columnNameList", new ArrayList<>());
        when(ColumnBuildUtil.handleColumnList(anyList(), anyList(), anyList()))
                .thenCallRealMethod();
        when(jdbcDialect.getRawTypeConverter()).thenReturn(JdbcRawTypeConverterTest::apply);

        inputFormat.openInternal(inputSplit);
    }

    @Test
    public void reachedEndTest()
            throws SQLException, InvocationTargetException, IllegalAccessException {
        when(inputFormat.reachedEnd()).thenCallRealMethod();
        when(inputFormat.getConnection()).thenCallRealMethod();
        doCallRealMethod().when(inputFormat).openNextSource();
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet resultSet = mock(ResultSet.class);
        ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);
        String sql = "select id,name from table";

        setInternalState(inputFormat, "noDataSource", true);
        Assert.assertTrue(inputFormat.reachedEnd());

        setInternalState(inputFormat, "noDataSource", false);
        setInternalState(inputFormat, "sourceList", dataSourceConfigList);
        setInternalState(inputFormat, "inputSplit", inputSplit);
        when(JdbcUtil.getConnection(any(JdbcConfig.class), any(JdbcDialect.class)))
                .thenAnswer(invocation -> connection);
        when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        when(jdbcConfig.getFetchSize()).thenReturn(100);
        when(jdbcConfig.getQueryTimeOut()).thenReturn(100);
        Method getSplitRangeFromDb =
                PowerMockito.method(
                        DistributedJdbcInputFormat.class,
                        "buildQuerySql",
                        DistributedJdbcInputSplit.class);
        when(getSplitRangeFromDb.invoke(inputFormat, any(DistributedJdbcInputSplit.class)))
                .thenReturn(sql);
        when(statement.executeQuery(sql)).thenReturn(resultSet);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(2);
        when(resultSet.next()).thenReturn(false, true);

        setInternalState(inputFormat, "sourceIndex", 1);
        Assert.assertTrue(inputFormat.reachedEnd());

        setInternalState(inputFormat, "sourceIndex", 0);
        doCallRealMethod().when(inputFormat).closeInternal();
        Assert.assertFalse(inputFormat.reachedEnd());

        setInternalState(inputFormat, "dbConn", (Object) null);
        when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenThrow(new SQLException(""));
        Assert.assertThrows(ChunJunRuntimeException.class, () -> inputFormat.reachedEnd());
    }
}
