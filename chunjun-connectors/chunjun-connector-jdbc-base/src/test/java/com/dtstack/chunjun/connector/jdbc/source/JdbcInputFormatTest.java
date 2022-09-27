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

package com.dtstack.chunjun.connector.jdbc.source;

import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.util.SqlUtil;
import com.dtstack.chunjun.connector.jdbc.util.key.NumericTypeUtil;
import com.dtstack.chunjun.constants.Metrics;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.enums.ColumnType;
import com.dtstack.chunjun.metrics.AccumulatorCollector;
import com.dtstack.chunjun.metrics.BigIntegerAccumulator;
import com.dtstack.chunjun.metrics.CustomReporter;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.ColumnBuildUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.doCallRealMethod;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.reflect.Whitebox.setInternalState;

/** @author liuliu 2022/4/15 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
    JdbcInputFormat.class,
    JdbcConf.class,
    JdbcDialect.class,
    JdbcInputSplit.class,
    SqlUtil.class,
    TableUtil.class,
    ColumnBuildUtil.class,
    CustomReporter.class,
    RuntimeContext.class,
    AccumulatorCollector.class,
    FormatState.class,
    BigIntegerAccumulator.class,
    TimeUnit.class,
    RowData.class,
    AbstractRowConverter.class
})
public class JdbcInputFormatTest {

    JdbcInputFormat jdbcInputFormat;
    JdbcDialect jdbcDialect;
    JdbcConf jdbcConf;

    AccumulatorCollector accumulatorCollector;
    BigIntegerAccumulator endLocationAccumulator;
    FormatState formatState;
    JdbcColumnConverter rowConverter;

    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet resultSet = mock(ResultSet.class);
    ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);

    @Before
    public void setup() {
        mockStatic(SqlUtil.class);
        mockStatic(TableUtil.class);
        mockStatic(ColumnBuildUtil.class);
        mockStatic(TimeUnit.class);

        jdbcInputFormat = mock(JdbcInputFormat.class);
        jdbcDialect = mock(JdbcDialect.class);
        Logger LOG = mock(Logger.class);
        jdbcConf = mock(JdbcConf.class);
        CustomReporter customReporter = mock(CustomReporter.class);
        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        accumulatorCollector = mock(AccumulatorCollector.class);
        endLocationAccumulator = mock(BigIntegerAccumulator.class);
        formatState = mock(FormatState.class);
        rowConverter = mock(JdbcColumnConverter.class);

        setInternalState(jdbcInputFormat, "LOG", LOG);
        setInternalState(jdbcInputFormat, "jdbcConf", jdbcConf);
        setInternalState(jdbcInputFormat, "jdbcDialect", jdbcDialect);
        setInternalState(jdbcInputFormat, "customReporter", customReporter);
        setInternalState(jdbcInputFormat, "accumulatorCollector", accumulatorCollector);
        setInternalState(jdbcInputFormat, "endLocationAccumulator", endLocationAccumulator);
        setInternalState(jdbcInputFormat, "formatState", formatState);
        setInternalState(jdbcInputFormat, "rowConverter", rowConverter);
        when(jdbcInputFormat.getRuntimeContext()).thenReturn(runtimeContext);
        when(jdbcConf.getStartLocation()).thenReturn("10");
    }

    /** -------------------------------- split test -------------------------------- */
    @Test
    public void createSplitWithErrorTest() {
        when(jdbcInputFormat.createInputSplitsInternal(2)).thenCallRealMethod();
        when(jdbcConf.getParallelism()).thenReturn(3);
        Assert.assertThrows(
                ChunJunRuntimeException.class, () -> jdbcInputFormat.createInputSplitsInternal(2));
    }

    @Test
    public void createModSplitTest() {
        when(jdbcInputFormat.createInputSplitsInternal(3)).thenCallRealMethod();
        when(jdbcInputFormat.createSplitsInternalBySplitMod(3, "20")).thenCallRealMethod();
        when(jdbcConf.getParallelism()).thenReturn(3);
        when(jdbcConf.getSplitStrategy()).thenReturn("mod");
        when(jdbcConf.getStartLocation()).thenReturn("20");
        Assert.assertEquals(jdbcInputFormat.createInputSplitsInternal(3).length, 3);
    }

    @Test
    public void createModSplitTestWithoutLocation() {
        when(jdbcInputFormat.createInputSplitsInternal(3)).thenCallRealMethod();
        when(jdbcInputFormat.createSplitsInternalBySplitMod(3, null)).thenCallRealMethod();
        when(jdbcConf.getParallelism()).thenReturn(3);
        when(jdbcConf.getSplitStrategy()).thenReturn("mod");
        when(jdbcConf.getStartLocation()).thenReturn(null);
        Assert.assertEquals(jdbcInputFormat.createInputSplitsInternal(3).length, 3);
    }

    @Test
    public void createModSplitMultiStartLocationTest() {
        when(jdbcInputFormat.createInputSplitsInternal(3)).thenCallRealMethod();
        when(jdbcInputFormat.createSplitsInternalBySplitMod(3, "30,40,50")).thenCallRealMethod();
        when(jdbcConf.getParallelism()).thenReturn(3);
        when(jdbcConf.getSplitStrategy()).thenReturn("mod");
        when(jdbcConf.getStartLocation()).thenReturn("30,40,50");
        Assert.assertEquals(jdbcInputFormat.createInputSplitsInternal(3).length, 3);
    }

    @Test
    public void createModSplitErrorTest() {
        when(jdbcInputFormat.createInputSplitsInternal(3)).thenCallRealMethod();
        when(jdbcInputFormat.createSplitsInternalBySplitMod(3, "30,40")).thenCallRealMethod();
        when(jdbcConf.getParallelism()).thenReturn(3);
        when(jdbcConf.getSplitStrategy()).thenReturn("mod");
        when(jdbcConf.getStartLocation()).thenReturn("30,40");
        Assert.assertThrows(
                ChunJunRuntimeException.class, () -> jdbcInputFormat.createInputSplitsInternal(3));
    }

    @Test
    public void createSplitsInternalBySplitRangeTest()
            throws InvocationTargetException, IllegalAccessException {
        when(jdbcInputFormat.createInputSplitsInternal(3)).thenCallRealMethod();
        when(jdbcInputFormat.createSplitsInternalBySplitRange(3)).thenCallRealMethod();
        when(jdbcInputFormat.createRangeSplits(
                        any(BigDecimal.class), any(BigDecimal.class), any(int.class)))
                .thenCallRealMethod();
        when(jdbcConf.getParallelism()).thenReturn(3);
        when(jdbcConf.getSplitStrategy()).thenReturn("range");
        when(jdbcConf.getStartLocation()).thenReturn("20");
        setInternalState(jdbcInputFormat, "splitKeyUtil", new NumericTypeUtil());

        Method getSplitRangeFromDb =
                PowerMockito.method(JdbcInputFormat.class, "getSplitRangeFromDb");
        Pair<String, String> pair = Pair.of("10", "30");
        when(getSplitRangeFromDb.invoke(jdbcInputFormat)).thenReturn(pair);

        Assert.assertEquals(jdbcInputFormat.createInputSplitsInternal(3).length, 3);
    }

    /** -------------------------------- openInternal test -------------------------------- */
    @Test
    public void openInternalTest() throws SQLException {
        JdbcInputSplit inputSplit =
                new JdbcInputSplit(1, 2, 1, null, null, null, null, "range", false);
        JdbcColumnConverter converter = mock(JdbcColumnConverter.class);

        setInternalState(jdbcInputFormat, "resultSet", resultSet);
        when(resultSet.isClosed()).thenReturn(false);
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        when(resultSetMetaData.getColumnCount()).thenReturn(1);
        setInternalState(jdbcInputFormat, "rowConverter", converter);

        when(jdbcConf.isIncrement()).thenReturn(true);
        when(jdbcConf.isPolling()).thenReturn(false);
        when(jdbcConf.isUseMaxFunc()).thenReturn(false);
        when(jdbcConf.getIncreColumnType()).thenReturn("int");

        when(jdbcInputFormat.getConnection()).thenReturn(connection);
        when(ColumnBuildUtil.handleColumnList(anyList(), anyList(), anyList()))
                .thenAnswer(invocation -> Pair.of(new ArrayList<>(), new ArrayList<>()));
        when(jdbcConf.getColumn()).thenReturn(new ArrayList<>());
        when(jdbcDialect.getRawTypeConverter()).thenReturn(null);
        when(TableUtil.createRowType(new ArrayList<>(), new ArrayList<>(), null))
                .thenAnswer(invocation -> null);

        when(jdbcInputFormat.canReadData(inputSplit)).thenReturn(true);
        doCallRealMethod().when(jdbcInputFormat).openInternal(inputSplit);
        jdbcInputFormat.openInternal(inputSplit);
        verify(jdbcInputFormat, times(3)).openInternal(any(JdbcInputSplit.class));

        doCallRealMethod().when(jdbcInputFormat).executeQuery(any());
        when(jdbcConf.isPolling()).thenReturn(false);
        when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenThrow(new SQLException());
        Assert.assertThrows(Exception.class, () -> jdbcInputFormat.openInternal(inputSplit));
    }

    @Test
    public void openInternalTestWithNoData() {
        JdbcInputSplit inputSplit =
                new JdbcInputSplit(1, 2, 1, null, null, null, null, "range", false);
        doCallRealMethod().when(jdbcInputFormat).openInternal(inputSplit);
        jdbcInputFormat.openInternal(inputSplit);
        verify(jdbcInputFormat, times(3)).openInternal(any(JdbcInputSplit.class));
    }

    @Test
    public void canReadDataTest() {
        JdbcInputSplit inputSplit = mock(JdbcInputSplit.class);
        JdbcInputSplit currentJdbcInputSplit = mock(JdbcInputSplit.class);
        setInternalState(jdbcInputFormat, "currentJdbcInputSplit", currentJdbcInputSplit);
        when(jdbcInputFormat.canReadData(inputSplit)).thenCallRealMethod();
        Assert.assertTrue(jdbcInputFormat.canReadData(inputSplit));

        when(jdbcConf.isIncrement()).thenReturn(true);
        when(currentJdbcInputSplit.isPolling()).thenReturn(false);

        when(inputSplit.getStartLocation()).thenReturn(null);
        when(inputSplit.getEndLocation()).thenReturn(null);
        Assert.assertTrue(jdbcInputFormat.canReadData(inputSplit));

        when(inputSplit.getStartLocation()).thenReturn("10");
        when(inputSplit.getEndLocation()).thenReturn("10");
        Assert.assertFalse(jdbcInputFormat.canReadData(inputSplit));
    }

    @Test
    public void initMetricTest() {
        doCallRealMethod().when(jdbcInputFormat).initMetric(any(JdbcInputSplit.class));

        JdbcInputSplit split = mock(JdbcInputSplit.class);
        jdbcInputFormat.initMetric(split);

        when(split.getStartLocation()).thenReturn("10");
        when(jdbcConf.isIncrement()).thenReturn(true);
        when(jdbcConf.getIncreColumnType()).thenReturn("int");
        when(jdbcConf.isPolling()).thenReturn(false);
        when(jdbcConf.isUseMaxFunc()).thenReturn(false);
        when(jdbcConf.getParallelism()).thenReturn(3);
        when(jdbcInputFormat.useCustomReporter()).thenReturn(true);
        jdbcInputFormat.initMetric(split);

        verify(jdbcInputFormat, times(6)).initMetric(any(JdbcInputSplit.class));
    }

    @Test
    public void getMaxValueTest() throws InvocationTargetException, IllegalAccessException {
        JdbcInputSplit split = mock(JdbcInputSplit.class);
        doCallRealMethod().when(split).setEndLocation("100");
        doCallRealMethod().when(jdbcInputFormat).getMaxValue(split);
        when(split.getSplitNumber()).thenReturn(0);
        Method getMaxValueFromDb = PowerMockito.method(JdbcInputFormat.class, "getMaxValueFromDb");
        when(getMaxValueFromDb.invoke(jdbcInputFormat)).thenReturn("100");
        jdbcInputFormat.getMaxValue(split);
        Assert.assertEquals(jdbcInputFormat.maxValueAccumulator.getLocalValue(), "100");

        when(split.getSplitNumber()).thenReturn(1);
        when(accumulatorCollector.getAccumulatorValue(Metrics.MAX_VALUE, true)).thenReturn(100L);
        jdbcInputFormat.getMaxValue(split);
    }

    @Test
    public void getMaxValueFromDbTest()
            throws InvocationTargetException, IllegalAccessException, SQLException {

        Method getMaxValueFromDb = PowerMockito.method(JdbcInputFormat.class, "getMaxValueFromDb");
        when(getMaxValueFromDb.invoke(jdbcInputFormat)).thenCallRealMethod();

        when(jdbcConf.getIncreColumn()).thenReturn("id");
        when(jdbcDialect.quoteIdentifier("id")).thenCallRealMethod();
        when(jdbcConf.getStartLocation()).thenReturn("10");
        when(jdbcConf.isUseMaxFunc()).thenReturn(true);
        when(jdbcConf.isPolling()).thenReturn(true);
        when(jdbcInputFormat.buildStartLocationSql("\"id\"", "10", true, true))
                .thenReturn("id >= 10");

        when(jdbcInputFormat.getConnection()).thenReturn(connection);
        when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                .thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getObject("max_value")).thenReturn("100");
        when(resultSet.getString("max_value")).thenReturn("100");

        setInternalState(jdbcInputFormat, "incrementKeyUtil", new NumericTypeUtil());
        jdbcConf.setCustomSql("");
        setInternalState(jdbcInputFormat, "type", ColumnType.INTEGER);
        when(jdbcConf.getCustomSql()).thenCallRealMethod();
        Assert.assertEquals(getMaxValueFromDb.invoke(jdbcInputFormat), "100");

        when(jdbcConf.getCustomSql()).thenReturn("select id from table");
        Assert.assertEquals(getMaxValueFromDb.invoke(jdbcInputFormat), "100");

        when(statement.executeQuery(anyString())).thenThrow(new SQLException());
        Assert.assertThrows(Exception.class, () -> getMaxValueFromDb.invoke(jdbcInputFormat));
    }

    @Test
    public void buildStartLocationSqlTest() {
        when(jdbcInputFormat.buildStartLocationSql(
                        anyString(), anyString(), anyBoolean(), anyBoolean()))
                .thenCallRealMethod();
        Assert.assertNull(jdbcInputFormat.buildStartLocationSql("id", "", true, true));
        Assert.assertEquals(
                "id >= ?", jdbcInputFormat.buildStartLocationSql("id", "10", true, true));

        when(jdbcInputFormat.getLocationSql("id", "10", " >= ")).thenCallRealMethod();
        setInternalState(jdbcInputFormat, "incrementKeyUtil", new NumericTypeUtil());
        Assert.assertEquals(
                "id >= 10", jdbcInputFormat.buildStartLocationSql("id", "10", true, false));
    }

    @Test
    public void buildQuerySqlTest() {
        setInternalState(jdbcInputFormat, "columnNameList", new ArrayList<>());
        JdbcInputSplit inputSplit = mock(JdbcInputSplit.class);
        when(jdbcInputFormat.buildQuerySql(inputSplit)).thenCallRealMethod();
        when(jdbcConf.getWhere()).thenReturn("id > 10");
        when(jdbcInputFormat.buildQuerySqlBySplit(any(JdbcInputSplit.class), anyList()))
                .thenCallRealMethod();
        when(SqlUtil.buildQuerySqlBySplit(any(), any(), anyList(), anyList(), any()))
                .thenAnswer(invocation -> "select id from table where id > 10");
        when(SqlUtil.buildOrderSql(jdbcConf, jdbcDialect, "ASC"))
                .thenAnswer(invocation -> " order by id ASC");
        String except = "select id from table where id > 10 order by id ASC";
        Assert.assertEquals(except, jdbcInputFormat.buildQuerySql(inputSplit));
    }

    @Test
    public void buildLocationFilterTest() {
        JdbcInputSplit inputSplit =
                new JdbcInputSplit(0, 1, 0, "10", "100", null, null, "mod", false);
        List<String> whereList = new ArrayList<>();
        doCallRealMethod().when(jdbcInputFormat).buildLocationFilter(inputSplit, whereList);
        setInternalState(jdbcInputFormat, "jdbcConf", jdbcConf);

        setInternalState(jdbcInputFormat, "incrementKeyUtil", new NumericTypeUtil());
        setInternalState(jdbcInputFormat, "restoreKeyUtil", new NumericTypeUtil());

        doAnswer(invocation -> true).when(jdbcConf).isIncrement();
        jdbcInputFormat.buildLocationFilter(inputSplit, whereList);
        when(jdbcInputFormat.buildFilterSql(
                        anyString(), anyString(), anyString(), anyBoolean(), anyString()))
                .thenCallRealMethod();

        when(jdbcConf.isUseMaxFunc()).thenReturn(true);
        when(jdbcDialect.quoteIdentifier(anyString())).thenCallRealMethod();

        // from state
        when(jdbcConf.getCustomSql()).thenReturn("");
        when(formatState.getState()).thenReturn(20);
        when(jdbcConf.getRestoreColumn()).thenReturn("id");

        when(jdbcConf.isIncrement()).thenReturn(true);
        jdbcInputFormat.buildLocationFilter(inputSplit, whereList);
        Assert.assertEquals(inputSplit.getStartLocation(), "20");
        // increment

        when(jdbcConf.getCustomSql()).thenReturn("select id from table");
        when(jdbcConf.getIncreColumn()).thenReturn("id");
        when(jdbcConf.getIncreColumnType()).thenReturn("int");
        when(formatState.getState()).thenReturn(null);
        when(jdbcConf.isPolling()).thenReturn(true);
        jdbcInputFormat.buildLocationFilter(inputSplit, whereList);
        Assert.assertTrue(whereList.contains("chunjun_tmp.\"id\" < 100"));
    }

    @Test
    public void executeQueryTest() throws SQLException {
        JdbcInputSplit split = mock(JdbcInputSplit.class);
        setInternalState(jdbcInputFormat, "restoreKeyUtil", new NumericTypeUtil());
        setInternalState(jdbcInputFormat, "currentJdbcInputSplit", split);
        setInternalState(jdbcInputFormat, "dbConn", connection);
        when(connection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(ps);
        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        doCallRealMethod().when(jdbcInputFormat).executeQuery(anyString());
        doCallRealMethod().when(jdbcInputFormat).initPrepareStatement(anyString());

        when(jdbcConf.getQuerySql()).thenReturn("select id from table");
        when(jdbcDialect.quoteIdentifier(anyString())).thenCallRealMethod();

        when(split.isPolling()).thenReturn(true, true, false);
        // polling
        jdbcInputFormat.executeQuery("20");
        jdbcInputFormat.executeQuery("");
        // increment
        jdbcInputFormat.executeQuery("");
    }
}
