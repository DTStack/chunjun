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

package com.dtstack.chunjun.connector.jdbc.sink;

import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcRawTypeConverterTest;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.jdbc.util.SqlUtil;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.enums.EWriteMode;
import com.dtstack.chunjun.enums.Semantic;
import com.dtstack.chunjun.metrics.AccumulatorCollector;
import com.dtstack.chunjun.metrics.BigIntegerAccumulator;
import com.dtstack.chunjun.metrics.CustomReporter;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.powermock.api.mockito.PowerMockito.doCallRealMethod;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.reflect.Whitebox.setInternalState;

/** @author liuliu 2022/8/22 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    JdbcOutputFormat.class,
    JdbcConf.class,
    JdbcDialect.class,
    SqlUtil.class,
    TableUtil.class,
    JdbcUtil.class,
    PreparedStmtProxy.class,
    FieldNamedPreparedStatement.class,
    LongCounter.class
})
public class JdbcOutputFormatTest {
    JdbcOutputFormat jdbcOutputFormat;
    JdbcDialect jdbcDialect;
    JdbcConf jdbcConf;

    AccumulatorCollector accumulatorCollector;
    BigIntegerAccumulator endLocationAccumulator;
    FormatState formatState;
    JdbcColumnConverter rowConverter;
    PreparedStmtProxy stmtProxy;
    LongCounter snapshotWriteCounter;

    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);

    @Before
    public void setup() {
        mockStatic(SqlUtil.class);
        mockStatic(JdbcUtil.class);
        mockStatic(TableUtil.class);
        mockStatic(FieldNamedPreparedStatement.class);

        jdbcOutputFormat = mock(JdbcOutputFormat.class);
        jdbcDialect = mock(JdbcDialect.class);
        Logger LOG = mock(Logger.class);
        jdbcConf = mock(JdbcConf.class);
        CustomReporter customReporter = mock(CustomReporter.class);
        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        accumulatorCollector = mock(AccumulatorCollector.class);
        endLocationAccumulator = mock(BigIntegerAccumulator.class);
        formatState = mock(FormatState.class);
        rowConverter = mock(JdbcColumnConverter.class);
        stmtProxy = mock(PreparedStmtProxy.class);
        snapshotWriteCounter = mock(LongCounter.class);

        setInternalState(jdbcOutputFormat, "dbConn", connection);
        setInternalState(jdbcOutputFormat, "formatState", formatState);
        setInternalState(jdbcOutputFormat, "jdbcConf", jdbcConf);
        setInternalState(jdbcOutputFormat, "jdbcDialect", jdbcDialect);
        setInternalState(jdbcOutputFormat, "stmtProxy", stmtProxy);
        setInternalState(jdbcOutputFormat, "snapshotWriteCounter", snapshotWriteCounter);
    }

    /** -------------------------------- openInternal test -------------------------------- */
    @Test
    public void openInternalTest() throws SQLException {
        doCallRealMethod().when(jdbcOutputFormat).openInternal(anyInt(), anyInt());
        when(jdbcOutputFormat.getConnection()).thenReturn(connection);
        setInternalState(jdbcOutputFormat, "semantic", Semantic.EXACTLY_ONCE);
        when(jdbcConf.getMode()).thenReturn(EWriteMode.UPDATE.name());
        when(jdbcConf.getUniqueKey()).thenReturn(new ArrayList<>());
        when(jdbcConf.getSchema()).thenReturn("test_schema");
        when(jdbcConf.getTable()).thenReturn("test_sink");
        when(JdbcUtil.getTableIndex("test_schema", "test_sink", connection))
                .thenAnswer(invocation -> Collections.singletonList("id"));
        jdbcOutputFormat.openInternal(1, 1);

        when(jdbcOutputFormat.getConnection()).thenThrow(new SQLException());
        Assert.assertThrows(Exception.class, () -> jdbcOutputFormat.openInternal(1, 1));
    }

    @Test
    public void buildStmtProxyTest() throws SQLException {
        doCallRealMethod().when(jdbcOutputFormat).buildStmtProxy();
        setInternalState(jdbcOutputFormat, "columnNameList", Collections.singletonList("id"));
        when(jdbcConf.getTable()).thenReturn("*");
        jdbcOutputFormat.buildStmtProxy();

        when(jdbcConf.getTable()).thenReturn("test_sink");
        when(jdbcDialect.getRawTypeConverter()).thenReturn(JdbcRawTypeConverterTest::apply);
        jdbcOutputFormat.buildStmtProxy();
    }

    /** -------------------------------- write test -------------------------------- */
    @Test
    public void writeSingleRecordInternalTest() throws Exception {
        doCallRealMethod().when(jdbcOutputFormat).writeSingleRecordInternal(any(RowData.class));

        jdbcOutputFormat.writeSingleRecordInternal(new ColumnRowData(1));

        doThrow(new SQLException("No operations allowed"))
                .when(stmtProxy)
                .writeSingleRecordInternal(any(RowData.class));
        doCallRealMethod()
                .when(jdbcOutputFormat)
                .processWriteException(any(), anyInt(), any(RowData.class));
        Assert.assertThrows(
                RuntimeException.class,
                () -> jdbcOutputFormat.writeSingleRecordInternal(new ColumnRowData(1)));
    }

    @Test
    public void processWriteExceptionTest() throws WriteRecordException {
        doCallRealMethod()
                .when(jdbcOutputFormat)
                .processWriteException(any(), anyInt(), any(RowData.class));
        doCallRealMethod()
                .when(jdbcOutputFormat)
                .recordConvertDetailErrorMessage(anyInt(), any(RowData.class));

        ColumnRowData columnRowData = new ColumnRowData(1);
        Assert.assertThrows(
                WriteRecordException.class,
                () -> jdbcOutputFormat.processWriteException(new Exception(""), 0, columnRowData));

        columnRowData.addField(new StringColumn(""));
        Assert.assertThrows(
                WriteRecordException.class,
                () -> jdbcOutputFormat.processWriteException(new Exception(""), 0, columnRowData));
    }

    @Test
    public void writeMultipleRecordsInternalTest() throws Exception {
        doCallRealMethod().when(jdbcOutputFormat).writeMultipleRecordsInternal();
        List<RowData> rows = new ArrayList<>();
        rows.add(new ColumnRowData(1));
        setInternalState(jdbcOutputFormat, "rows", rows);
        setInternalState(jdbcOutputFormat, "semantic", Semantic.EXACTLY_ONCE);
        jdbcOutputFormat.writeMultipleRecordsInternal();

        when(stmtProxy.executeBatch()).thenThrow(new SQLException());
        Assert.assertThrows(
                SQLException.class, () -> jdbcOutputFormat.writeMultipleRecordsInternal());
    }

    @Test
    public void preCommitTest() throws Exception {
        doCallRealMethod().when(jdbcOutputFormat).preCommit();
        when(jdbcConf.getRestoreColumnIndex()).thenReturn(0);

        ColumnRowData columnRowData = new ColumnRowData(1);
        columnRowData.addField(new StringColumn("123"));
        setInternalState(jdbcOutputFormat, "lastRow", columnRowData);
        jdbcOutputFormat.preCommit();
    }

    @Test
    public void commitTest() throws Exception {
        doCallRealMethod().when(jdbcOutputFormat).commit(1);
        doCallRealMethod().when(jdbcOutputFormat).doCommit();
        setInternalState(jdbcOutputFormat, "rowsOfCurrentTransaction", 1);
        jdbcOutputFormat.commit(1);

        doThrow(new ChunJunRuntimeException("")).when(stmtProxy).clearStatementCache();
        Assert.assertThrows(Exception.class, () -> jdbcOutputFormat.commit(1));
    }

    @Test
    public void executeBatchTest() throws SQLException {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("truncate table test_sink");
        sqlList.add("truncate table test_sink");
        doCallRealMethod().when(jdbcOutputFormat).executeBatch(sqlList);

        when(jdbcOutputFormat.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        jdbcOutputFormat.executeBatch(sqlList);

        when(statement.executeBatch()).thenThrow(new SQLException(""));
        Assert.assertThrows(RuntimeException.class, () -> jdbcOutputFormat.executeBatch(sqlList));
    }

    @Test
    public void prepareTemplatesTest() {
        when(jdbcOutputFormat.prepareTemplates()).thenCallRealMethod();
        when(jdbcConf.getSchema()).thenReturn("test_schema");
        when(jdbcConf.getTable()).thenReturn("test_sink");

        List<String> columnNameList = new ArrayList<>();
        columnNameList.add("id");
        columnNameList.add("name");
        setInternalState(jdbcOutputFormat, "columnNameList", columnNameList);

        String expect;
        // insert
        when(jdbcConf.getMode()).thenReturn(EWriteMode.INSERT.name());
        when(jdbcDialect.getInsertIntoStatement(any(), any(), any())).thenCallRealMethod();
        expect = "INSERT INTO null(null, null) VALUES (:id, :name)";
        Assert.assertEquals(expect, jdbcOutputFormat.prepareTemplates());
        // replace
        when(jdbcConf.getMode()).thenReturn(EWriteMode.REPLACE.name());
        when(jdbcDialect.getReplaceStatement(any(), any(), any())).thenCallRealMethod();
        Assert.assertThrows(
                NoSuchElementException.class, () -> jdbcOutputFormat.prepareTemplates());
        // update
        when(jdbcConf.getMode()).thenReturn(EWriteMode.UPDATE.name());
        when(jdbcDialect.getUpdateStatement(any(), any(), any(), any())).thenCallRealMethod();
        when(jdbcConf.getUniqueKey()).thenReturn(Collections.singletonList("id"));
        when(jdbcConf.isAllReplace()).thenReturn(true);
        Assert.assertThrows(
                NoSuchElementException.class, () -> jdbcOutputFormat.prepareTemplates());
        // exception
        when(jdbcConf.getMode()).thenReturn("asd");
        Assert.assertThrows(
                IllegalArgumentException.class, () -> jdbcOutputFormat.prepareTemplates());
    }

    @Test
    public void closeInternalTest() throws SQLException {
        doCallRealMethod().when(jdbcOutputFormat).closeInternal();
        setInternalState(jdbcOutputFormat, "rowsOfCurrentTransaction", 1);
        jdbcOutputFormat.closeInternal();

        doThrow(new SQLException("")).when(stmtProxy).close();
        jdbcOutputFormat.closeInternal();
    }
}
