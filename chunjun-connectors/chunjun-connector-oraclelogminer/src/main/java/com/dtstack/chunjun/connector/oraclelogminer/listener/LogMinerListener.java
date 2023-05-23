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

package com.dtstack.chunjun.connector.oraclelogminer.listener;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.DdlRowDataBuilder;
import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.connector.oraclelogminer.config.LogMinerConfig;
import com.dtstack.chunjun.connector.oraclelogminer.converter.LogMinerColumnConverter;
import com.dtstack.chunjun.connector.oraclelogminer.entity.ColumnInfo;
import com.dtstack.chunjun.connector.oraclelogminer.entity.QueueData;
import com.dtstack.chunjun.connector.oraclelogminer.util.OraUtil;
import com.dtstack.chunjun.connector.oraclelogminer.util.SqlUtil;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.ErrorMsgRowData;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.RetryUtil;

import org.apache.flink.table.data.RowData;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.oraclelogminer.listener.LogMinerConnection.RETRY_TIMES;
import static com.dtstack.chunjun.connector.oraclelogminer.listener.LogMinerConnection.SLEEP_TIME;

@Slf4j
public class LogMinerListener implements Runnable {
    private final BigInteger MINUS_ONE = new BigInteger("-1");

    private final LogMinerConfig logMinerConfig;
    private final PositionManager positionManager;
    private final AbstractCDCRawTypeMapper rowConverter;
    private final LogMinerHelper logMinerHelper;
    private BlockingQueue<QueueData> queue;
    private ExecutorService executor;
    private LogParser logParser;
    private boolean running = false;
    private final transient LogMinerListener listener;
    /** 连续接收到错误数据的次数 */
    private int failedTimes = 0;

    public LogMinerListener(
            LogMinerConfig logMinerConfig,
            PositionManager positionManager,
            AbstractCDCRawTypeMapper rowConverter) {
        this.positionManager = positionManager;
        this.logMinerConfig = logMinerConfig;
        this.listener = this;
        this.rowConverter = rowConverter;
        this.logMinerHelper = new LogMinerHelper(listener, logMinerConfig, null);
    }

    public void init() {
        queue = new LinkedBlockingDeque<>();

        ThreadFactory namedThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("LogMiner-pool-%d").build();
        executor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1024),
                        namedThreadFactory,
                        new ThreadPoolExecutor.AbortPolicy());

        logParser = new LogParser();
    }

    public void start() {
        BigInteger startScn;
        Connection connection =
                RetryUtil.executeWithRetry(
                        () ->
                                DriverManager.getConnection(
                                        logMinerConfig.getJdbcUrl(),
                                        logMinerConfig.getUsername(),
                                        logMinerConfig.getPassword()),
                        RETRY_TIMES,
                        SLEEP_TIME,
                        false);
        if (logMinerConfig.isEnableFetchAll()) {
            BigInteger cacheScn = positionManager.getScn();
            if (null != cacheScn && cacheScn.compareTo(BigInteger.valueOf(-1)) != 0) {
                startScn = cacheScn;
            } else {
                startScn = oracleFullSyncOperation(connection);
            }
        } else {
            startScn = logMinerHelper.getStartScn(positionManager.getScn());
        }

        positionManager.setScn(startScn);
        logMinerHelper.setStartScn(startScn);
        logMinerHelper.init();

        // LogMinerColumnConverter 需要connection获取元数据
        if (rowConverter instanceof LogMinerColumnConverter) {
            ((LogMinerColumnConverter) rowConverter).setConnection(connection);
        }

        // 初始化
        if (logMinerConfig.isInitialTableStructure() && !logMinerConfig.isEnableFetchAll()) {
            initialTableStruct(connection);
        }

        executor.execute(this);
        running = true;
    }

    @Override
    public void run() {
        Thread.currentThread()
                .setUncaughtExceptionHandler(
                        (t, e) -> {
                            log.warn(
                                    "LogMinerListener run failed, Throwable = {}",
                                    ExceptionUtil.getErrorMessage(e));
                            executor.execute(listener);
                            log.info("Re-execute LogMinerListener successfully");
                        });

        while (running) {
            QueueData log = null;
            try {
                if (logMinerHelper.hasNext()) {
                    log = logMinerHelper.getQueueData();
                    processData(log);
                }
            } catch (Exception e) {
                sendException(e, log);
                logMinerHelper.restart(e);
            }
        }
    }

    public void sendException(Exception e, QueueData queueData) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("LogMinerListener thread exception: current scn =")
                .append(positionManager.getScn());
        if (Objects.nonNull(queueData)) {
            sb.append(",\nqueueData = ").append(queueData);
        }
        sb.append(",\ne = ").append(ExceptionUtil.getErrorMessage(e));
        String msg = sb.toString();
        log.warn(msg);
        try {
            queue.put(new QueueData(BigInteger.ZERO, new ErrorMsgRowData(msg)));
            Thread.sleep(2000L);
        } catch (InterruptedException ex) {
            log.warn(
                    "error to put exception message into queue, e = {}",
                    ExceptionUtil.getErrorMessage(ex));
        }
    }

    public void stop() {
        if (null != executor && !executor.isShutdown()) {
            executor.shutdown();
            running = false;
        }

        if (null != queue) {
            queue.clear();
        }

        if (null != logMinerHelper) {
            logMinerHelper.stop();
        }
    }

    private void processData(QueueData queueData) throws Exception {
        if (queueData.getData() instanceof DdlRowData) {
            rowConverter.clearConverterCache();
            queue.put((new QueueData(queueData.getScn(), queueData.getData())));
            return;
        }
        LinkedList<RowData> rowDatalist = logParser.parse(queueData, rowConverter);
        RowData rowData;
        try {
            while ((rowData = rowDatalist.poll()) != null) {
                queue.put(new QueueData(queueData.getScn(), rowData));
            }
        } catch (Exception e) {
            log.error("{}", ExceptionUtil.getErrorMessage(e));
        }
    }

    public RowData getData() {
        RowData rowData = null;
        try {
            // 最多阻塞100ms
            QueueData poll = queue.poll(100, TimeUnit.MILLISECONDS);
            if (Objects.nonNull(poll)) {
                rowData = poll.getData();
                if (rowData instanceof ErrorMsgRowData) {
                    if (++failedTimes >= logMinerConfig.getRetryTimes()) {
                        String errorMsg = rowData.toString();
                        StringBuilder sb = new StringBuilder(errorMsg.length() + 128);
                        sb.append("Error data is received ")
                                .append(failedTimes)
                                .append(" times continuously, ");
                        Pair<String, String> pair = OraUtil.parseErrorMsg(errorMsg);
                        if (pair != null) {
                            sb.append("\nthe Cause maybe : ")
                                    .append(pair.getLeft())
                                    .append(", \nand the Solution maybe : ")
                                    .append(pair.getRight())
                                    .append(", ");
                        }
                        sb.append("\nerror msg is : ").append(errorMsg);
                        throw new RuntimeException(sb.toString());
                    }
                    rowData = null;
                } else {
                    if (poll.getScn().compareTo(MINUS_ONE) != 0) {
                        positionManager.setScn(poll.getScn());
                    }
                    failedTimes = 0;
                }
            }
        } catch (InterruptedException e) {
            log.warn("Get data from queue error:", e);
        }
        return rowData;
    }

    private void initialTableStruct(Connection connection) {
        if (CollectionUtils.isNotEmpty(logMinerConfig.getTable())) {
            Set<String> schemas = new HashSet<>();
            logMinerConfig
                    .getTable()
                    .forEach(
                            i -> {
                                String[] split = i.split("\\.");
                                if (split.length == 2) {
                                    schemas.add(split[0]);
                                }
                            });

            List<Pattern> patterns =
                    logMinerConfig.getTable().stream()
                            .map(Pattern::compile)
                            .collect(Collectors.toList());

            ArrayList<Pair<String, String>> tables = new ArrayList<>();
            HashMap<Pair<String, String>, List<String>> pkMap = new HashMap<>();
            schemas.forEach(
                    i -> {
                        try (final ResultSet rs =
                                connection
                                        .getMetaData()
                                        .getTables(null, i, null, new String[] {"TABLE"})) {
                            while (rs.next()) {

                                final String schemaName = rs.getString(2);
                                final String tableName = rs.getString(3);
                                if (patterns.stream()
                                        .anyMatch(
                                                f ->
                                                        f.matcher(schemaName + "." + tableName)
                                                                .matches())) {
                                    tables.add(Pair.of(schemaName, tableName));

                                    List<String> pks =
                                            readPrimaryKeyNames(
                                                    connection.getMetaData(),
                                                    schemaName,
                                                    tableName);
                                    pkMap.put(Pair.of(schemaName, tableName), pks);
                                }
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
            if (CollectionUtils.isNotEmpty(tables)) {
                List<List<Pair<String, String>>> partition =
                        Lists.partition(tables, tables.size() / 50 == 0 ? 1 : tables.size() / 50);
                for (List<Pair<String, String>> pairs : partition) {
                    try {
                        PreparedStatement preparedStatement =
                                connection.prepareStatement(SqlUtil.formatGetTableInfoSql(pairs));
                        int index = 0;
                        for (Pair<String, String> pair : pairs) {
                            preparedStatement.setString(++index, pair.getLeft());
                            preparedStatement.setString(++index, pair.getRight());
                        }

                        ResultSet resultSet = preparedStatement.executeQuery();
                        HashMap<Pair<String, String>, List<ColumnInfo>> tableMap = new HashMap<>();
                        HashMap<Pair<String, String>, String> commentMap = new HashMap<>();

                        while (resultSet.next()) {
                            String schema = resultSet.getString(1);
                            String tableName = resultSet.getString(2);
                            String columnName = resultSet.getString(3);
                            String dataType = resultSet.getString(4);
                            Object dataPrecision = resultSet.getObject(5);
                            Object charLength = resultSet.getObject(6);
                            Object dataLength = resultSet.getObject(7);
                            Object dataScale = resultSet.getObject(8);
                            Object defaultValue = resultSet.getObject(9);
                            String nullable = resultSet.getString(10);
                            String comment = resultSet.getString(11);
                            String tableComment = resultSet.getString(12);

                            Pair<String, String> key = Pair.of(schema, tableName);
                            List<String> pks = pkMap.get(key);
                            boolean isPk = false;
                            if (CollectionUtils.isNotEmpty(pks)) {
                                isPk = pks.contains(columnName);
                            }

                            ColumnInfo columnInfo =
                                    new ColumnInfo(
                                            columnName,
                                            dataType,
                                            Objects.isNull(dataPrecision)
                                                    ? null
                                                    : Integer.valueOf(dataPrecision.toString()),
                                            Objects.isNull(charLength)
                                                    ? null
                                                    : Integer.valueOf(charLength.toString()),
                                            Objects.isNull(dataLength)
                                                    ? null
                                                    : Integer.valueOf(dataLength.toString()),
                                            Objects.isNull(dataScale)
                                                    ? null
                                                    : Integer.valueOf(dataScale.toString()),
                                            Objects.isNull(defaultValue)
                                                    ? null
                                                    : defaultValue.toString(),
                                            "Y".equalsIgnoreCase(nullable),
                                            comment,
                                            isPk);

                            commentMap.putIfAbsent(key, tableComment);
                            if (tableMap.containsKey(key)) {
                                tableMap.get(key).add(columnInfo);
                            } else {
                                tableMap.put(key, Lists.newArrayList(columnInfo));
                            }
                        }

                        for (Map.Entry<Pair<String, String>, List<ColumnInfo>> entry :
                                tableMap.entrySet()) {
                            DdlRowData ddlData =
                                    DdlRowDataBuilder.builder()
                                            .setDatabaseName(null)
                                            .setSchemaName(entry.getKey().getLeft())
                                            .setTableName(entry.getKey().getRight())
                                            .setContent(
                                                    SqlUtil.getSql(
                                                            entry.getKey().getLeft(),
                                                            entry.getKey().getRight(),
                                                            entry.getValue()))
                                            .setType(EventType.CREATE_TABLE.name())
                                            .setLsn("")
                                            .setLsnSequence("0")
                                            .setSnapShot(true)
                                            .build();
                            queue.put(new QueueData(new BigInteger("-1"), ddlData));

                            List<String> comments =
                                    entry.getValue().stream()
                                            .filter(c -> StringUtils.isNotBlank(c.getComment()))
                                            .map(ColumnInfo::getCommentSql)
                                            .collect(Collectors.toList());
                            int lsnSequences = 1;
                            for (String comment : comments) {
                                DdlRowData commentDdlData =
                                        DdlRowDataBuilder.builder()
                                                .setDatabaseName(null)
                                                .setSchemaName(entry.getKey().getLeft())
                                                .setTableName(entry.getKey().getRight())
                                                .setContent(comment)
                                                .setType(EventType.ALTER_COLUMN.name())
                                                .setLsn("")
                                                .setLsnSequence(String.valueOf(lsnSequences++))
                                                .setSnapShot(true)
                                                .build();
                                queue.put(new QueueData(new BigInteger("-1"), commentDdlData));
                            }

                            if (commentMap.get(entry.getKey()) != null) {
                                DdlRowData commentDdlData =
                                        DdlRowDataBuilder.builder()
                                                .setDatabaseName(null)
                                                .setSchemaName(entry.getKey().getLeft())
                                                .setTableName(entry.getKey().getRight())
                                                .setContent(
                                                        SqlUtil.getTableCommentSql(
                                                                entry.getKey().getLeft(),
                                                                entry.getKey().getRight(),
                                                                commentMap.get(entry.getKey())))
                                                .setType(EventType.ALTER_TABLE_COMMENT.name())
                                                .setLsn("")
                                                .setLsnSequence(String.valueOf(lsnSequences++))
                                                .setSnapShot(true)
                                                .build();
                                queue.put(new QueueData(new BigInteger("-1"), commentDdlData));
                            }
                        }
                    } catch (SQLException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public BigInteger getCurrentPosition() {
        return positionManager.getScn();
    }

    private List<String> readPrimaryKeyNames(DatabaseMetaData metadata, String schema, String table)
            throws SQLException {
        final List<String> pkColumnNames = new ArrayList<>();
        try (ResultSet rs = metadata.getPrimaryKeys(null, schema, table)) {
            while (rs.next()) {
                String columnName = rs.getString(4);
                pkColumnNames.add(columnName);
            }
        }
        return pkColumnNames;
    }

    /**
     * lock table get current scn generate create table ddl release lock generate insert event by
     * scn
     */
    public BigInteger oracleFullSyncOperation(Connection connection) {
        try {
            if (logMinerConfig.getTable().size() != 1) {
                throw new ChunJunRuntimeException(
                        "oracle logminer full sync, only support one table");
            }

            String tbnWithSchema = logMinerConfig.getTable().get(0);
            BigInteger scn = getLockTableScn(connection, tbnWithSchema);

            /* select data by scn, to columnRowData */
            queryDataByScnToColumnRowData(connection, tbnWithSchema, scn);

            return scn;
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    private BigInteger getLockTableScn(Connection conn, String tbnWithSchema) {
        try (Statement stmt = conn.createStatement()) {

            String lockTableSql = SqlUtil.formatLockTableWithRowShare(tbnWithSchema);

            /* lock table */
            stmt.execute(lockTableSql);

            /* get current scn */
            ResultSet rs = stmt.executeQuery(SqlUtil.SQL_GET_CURRENT_SCN);

            String scn = null;
            if (rs.next()) {
                scn = rs.getString("CURRENT_SCN");
            } else {
                throw new ChunJunRuntimeException(
                        String.format("can't get scn of [%s]", tbnWithSchema));
            }

            /* generate create table ddl */
            if (logMinerConfig.isInitialTableStructure()) {
                initialTableStruct(conn);
            }

            /* release lock */
            stmt.execute(SqlUtil.releaseTableLock());

            return new BigInteger(scn);
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    private void queryDataByScnToColumnRowData(
            Connection conn, String tbnWithSchema, BigInteger scn) {
        try {
            List<ColumnInfo> columnInfos = getColumnInfoByTable(conn, tbnWithSchema);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(SqlUtil.queryDataByScn(tbnWithSchema, scn));
            List<ColumnRowData> columnRowDatas =
                    SqlUtil.jdbcColumnRowColumnConvert(columnInfos, rs);

            for (ColumnRowData rowData : columnRowDatas) {
                queue.put(new QueueData(new BigInteger("-1"), rowData));
            }
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    private List<ColumnInfo> getColumnInfoByTable(Connection conn, String tbnWithSchema) {
        String schema = tbnWithSchema.substring(0, tbnWithSchema.indexOf('.'));
        String tbn = tbnWithSchema.substring(tbnWithSchema.indexOf('.') + 1);
        List<ColumnInfo> columnInfos = new ArrayList<>();

        try (Statement stmt = conn.createStatement();
                ResultSet resultSet =
                        stmt.executeQuery(SqlUtil.formatGetTableInfoSql(schema, tbn))) {

            while (resultSet.next()) {
                String columnName = resultSet.getString(3);
                String dataType = resultSet.getString(4);
                Object dataPrecision = resultSet.getObject(5);
                Object charLength = resultSet.getObject(6);
                Object dataLength = resultSet.getObject(7);
                Object dataScale = resultSet.getObject(8);
                Object defaultValue = resultSet.getObject(9);
                String nullable = resultSet.getString(10);
                String comment = resultSet.getString(11);

                /* isPk is not useful, set false */
                boolean isPk = false;

                ColumnInfo columnInfo =
                        new ColumnInfo(
                                columnName,
                                dataType,
                                Objects.isNull(dataPrecision)
                                        ? null
                                        : Integer.valueOf(dataPrecision.toString()),
                                Objects.isNull(charLength)
                                        ? null
                                        : Integer.valueOf(charLength.toString()),
                                Objects.isNull(dataLength)
                                        ? null
                                        : Integer.valueOf(dataLength.toString()),
                                Objects.isNull(dataScale)
                                        ? null
                                        : Integer.valueOf(dataScale.toString()),
                                Objects.isNull(defaultValue) ? null : defaultValue.toString(),
                                "Y".equalsIgnoreCase(nullable),
                                comment,
                                isPk);

                columnInfos.add(columnInfo);
            }
            return columnInfos;
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }
}
