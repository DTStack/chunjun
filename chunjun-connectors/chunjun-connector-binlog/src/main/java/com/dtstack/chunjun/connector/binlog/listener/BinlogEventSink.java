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
package com.dtstack.chunjun.connector.binlog.listener;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.DdlRowDataBuilder;
import com.dtstack.chunjun.cdc.EventType;
import com.dtstack.chunjun.connector.binlog.config.BinlogConfig;
import com.dtstack.chunjun.connector.binlog.inputformat.BinlogInputFormat;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.element.ErrorMsgRowData;
import com.dtstack.chunjun.throwable.WriteRecordException;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.RetryUtil;

import org.apache.flink.table.data.RowData;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class BinlogEventSink extends AbstractCanalLifeCycle
        implements com.alibaba.otter.canal.sink.CanalEventSink<List<CanalEntry.Entry>> {

    private final BinlogInputFormat format;
    private final LinkedBlockingDeque<RowData> queue;
    private final AbstractCDCRawTypeMapper rowConverter;

    private final String OFFSET_LENGTH;

    public BinlogEventSink(BinlogInputFormat format) {
        this.format = format;
        this.queue = new LinkedBlockingDeque<>();
        this.rowConverter = format.getCdcRowConverter();
        this.OFFSET_LENGTH = "%0" + this.format.getBinlogConfig().getOffsetLength() + "d";
    }

    public void initialTableStructData(List<String> pattern) {

        if (CollectionUtils.isNotEmpty(pattern)) {

            Set<String> schemas = new HashSet<>();

            pattern.forEach(
                    i -> {
                        String[] split = i.split("\\.");
                        if (split.length == 2) {
                            schemas.add(split[0]);
                        }
                    });

            List<Pattern> patterns =
                    pattern.stream().map(Pattern::compile).collect(Collectors.toList());

            Connection connection = getConnection();

            ArrayList<Pair<String, String>> tables = new ArrayList<>();
            schemas.forEach(
                    i -> {
                        try (final ResultSet rs =
                                connection
                                        .getMetaData()
                                        .getTables(i, null, null, new String[] {"TABLE"})) {
                            while (rs.next()) {
                                final String catalogName = rs.getString(1);
                                final String tableName = rs.getString(3);
                                if (patterns.stream()
                                        .anyMatch(
                                                f ->
                                                        f.matcher(catalogName + "." + tableName)
                                                                .matches())) {
                                    tables.add(Pair.of(catalogName, tableName));
                                }
                            }
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });

            tables.forEach(
                    i -> {
                        try {
                            PreparedStatement preparedStatement =
                                    connection.prepareStatement(
                                            String.format(
                                                    "show create table %s.%s",
                                                    i.getLeft(), i.getRight()));
                            ResultSet resultSet = preparedStatement.executeQuery();
                            resultSet.next();
                            String ddl = resultSet.getString(2);
                            DdlRowData ddlData =
                                    DdlRowDataBuilder.builder()
                                            .setDatabaseName(null)
                                            .setSchemaName(i.getLeft())
                                            .setTableName(i.getRight())
                                            .setContent(ddl)
                                            .setType(EventType.CREATE_TABLE.name())
                                            .setLsn("")
                                            .setLsnSequence("0")
                                            .setSnapShot(true)
                                            .build();
                            queue.put(ddlData);

                        } catch (SQLException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
            if (CollectionUtils.isNotEmpty(tables)) {
                log.info(
                        "snapshot table struct {}",
                        tables.stream()
                                .map(i -> "[" + i.getLeft() + "." + i.getRight() + "]")
                                .collect(Collectors.joining(",")));
            }
        }
    }

    @Override
    public boolean sink(
            List<CanalEntry.Entry> entries, InetSocketAddress inetSocketAddress, String s)
            throws CanalSinkException {
        for (CanalEntry.Entry entry : entries) {
            CanalEntry.EntryType entryType = entry.getEntryType();
            if (entryType != CanalEntry.EntryType.ROWDATA) {
                continue;
            }
            CanalEntry.RowChange rowChange = null;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                log.error("parser data[{}] error:{}", entry, ExceptionUtil.getErrorMessage(e));
            }

            if (rowChange == null) {
                return false;
            }

            CanalEntry.Header header = entry.getHeader();
            String schema = header.getSchemaName();
            String table = header.getTableName();
            long executeTime = header.getExecuteTime();
            String lsn = buildLastPosition(entry);
            try {
                processRowChange(rowChange, schema, table, executeTime, lsn);
            } catch (WriteRecordException e) {
                // todo 脏数据记录
                if (log.isDebugEnabled()) {
                    log.debug(
                            "write error rowData, rowData = {}, e = {}",
                            e.getRowData().toString(),
                            ExceptionUtil.getErrorMessage(e));
                }
            }
        }
        return true;
    }

    /**
     * 处理RowData数据
     *
     * @param rowChange 解析后的RowData数据
     * @param schema schema
     * @param table table
     * @param executeTime 变更数据的执行时间
     */
    @SuppressWarnings("unchecked")
    private void processRowChange(
            CanalEntry.RowChange rowChange,
            String schema,
            String table,
            long executeTime,
            String lsn)
            throws WriteRecordException {
        String eventType = rowChange.getEventType().toString();
        List<String> categories = format.getCategories();
        if (CollectionUtils.isNotEmpty(categories) && !categories.contains(eventType)) {
            return;
        }
        BinlogEventRow binlogEventRow =
                new BinlogEventRow(rowChange, schema, table, executeTime, lsn);
        LinkedList<RowData> rowDatalist;
        try {
            rowDatalist = rowConverter.toInternal(binlogEventRow);
        } catch (Exception e) {
            throw new WriteRecordException("", e, 0, binlogEventRow);
        }
        RowData rowData = null;
        try {
            while (rowDatalist != null && (rowData = rowDatalist.poll()) != null) {
                queue.put(rowData);
            }
        } catch (InterruptedException e) {
            log.error(
                    "put rowData[{}] into queue interrupted error:{}",
                    rowData,
                    ExceptionUtil.getErrorMessage(e));
        }
    }

    /**
     * 从队列中获取RowData数据，对于异常情况需要把异常抛出并停止任务
     *
     * @return
     */
    public RowData takeRowDataFromQueue() {
        RowData rowData = null;
        try {
            // 最多阻塞100ms
            rowData = queue.poll(100, TimeUnit.MILLISECONDS);
            if (rowData instanceof ErrorMsgRowData) {
                throw new RuntimeException(rowData.toString());
            }
        } catch (InterruptedException e) {
            log.error(
                    "takeRowDataFromQueue interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return rowData;
    }

    /**
     * 处理异常数据
     *
     * @param rowData
     */
    public void processErrorMsgRowData(ErrorMsgRowData rowData) {
        try {
            queue.put(rowData);
        } catch (InterruptedException e) {
            log.error(
                    "processErrorMsgRowData interrupted rowData:{} error:{}",
                    rowData,
                    ExceptionUtil.getErrorMessage(e));
        }
    }

    protected String buildLastPosition(CanalEntry.Entry entry) {
        String pos = String.format(OFFSET_LENGTH, entry.getHeader().getLogfileOffset());
        return entry.getHeader().getLogfileName() + "/" + pos;
    }

    @Override
    public void interrupt() {
        log.warn("BinlogEventSink is interrupted");
    }

    private Connection getConnection() {
        BinlogConfig binlogConfig = format.getBinlogConfig();
        String jdbcUrl = binlogConfig.getJdbcUrl();
        String username = binlogConfig.getUsername();
        String password = binlogConfig.getPassword();
        Properties prop = new Properties();
        if (org.apache.commons.lang3.StringUtils.isNotBlank(username)) {
            prop.put("user", username);
        }
        if (org.apache.commons.lang3.StringUtils.isNotBlank(password)) {
            prop.put("password", password);
        }

        Connection connection;
        synchronized (ClassUtil.LOCK_STR) {
            connection =
                    RetryUtil.executeWithRetry(
                            () -> DriverManager.getConnection(jdbcUrl, prop), 3, 2000, false);
        }
        return connection;
    }
}
