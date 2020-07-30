/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.binlog.reader;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.dtstack.flinkx.binlog.BinlogJournalValidator;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.google.common.base.Joiner;
import com.util.DbUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * @author toutian
 */
public class BinlogInputFormat extends BaseRichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogInputFormat.class);

    private BinlogConfig binlogConfig;

    private volatile EntryPosition entryPosition;

    private List<String> categories = new ArrayList<>();

    private final String SCHEMA_SPLIT = ".";

    private final String AUTHORITY_TEMPLATE_SQL = "select count(1) from %s";

    /**
     * internal fields
     */
    private transient MysqlEventParser controller;

    private transient BinlogEventSink binlogEventSink;

    public void updateLastPos(EntryPosition entryPosition) {
        this.entryPosition = entryPosition;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();

        LOG.info("binlog configure...");

        if (StringUtils.isNotEmpty(binlogConfig.getCat())) {
            LOG.info("{}", categories);
            categories = Arrays.asList(binlogConfig.getCat().toUpperCase().split(","));
        }
        /**
         * mysql 数据解析关注的表，Perl正则表达式.

         多个正则之间以逗号(,)分隔，转义符需要双斜杠(\\)


         常见例子：

         1.  所有表：.*   or  .*\\..*
         2.  canal schema下所有表： canal\\..*
         3.  canal下的以canal打头的表：canal\\.canal.*
         4.  canal schema下的一张表：canal\\.test1

         5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)
         */
        List<String> tables = binlogConfig.getTable();
        String jdbcUrl = binlogConfig.getJdbcUrl();
        if (tables != null && tables.size() != 0 && jdbcUrl != null) {
            int idx = jdbcUrl.lastIndexOf('?');
            String database;
            if (idx != -1) {
                database = StringUtils.substring(jdbcUrl, jdbcUrl.lastIndexOf('/') + 1, idx);
            } else {
                database = StringUtils.substring(jdbcUrl, jdbcUrl.lastIndexOf('/') + 1);
            }
            HashMap<String, String> checkedTable = new HashMap<>(tables.size());
            //按照.切割字符串需要转义
            String regexSchemaSplit = "\\" + SCHEMA_SPLIT;
            String filter = tables.stream()
                    .map(t -> formatTableName(database, t))
                    //只需要每个schema下的一个表进行判断
                    .peek(t -> checkedTable.putIfAbsent(t.split(regexSchemaSplit)[0], t))
                    .collect(Collectors.joining(","));

            binlogConfig.setFilter(filter);

            //检验每个schema下的第一个表的权限
            checkSourceAuthority(checkedTable.values());
        }

    }

    @Override
    public FormatState getFormatState() {
        if (!restoreConfig.isRestore()) {
            LOG.info("return null for formatState");
            return null;
        }

        super.getFormatState();
        if (formatState != null) {
            formatState.setState(entryPosition);
        }
        return formatState;
    }

    public boolean accept(String type) {
        return categories.isEmpty() || categories.contains(type);
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        if (inputSplit.getSplitNumber() != 0) {
            LOG.info("binlog openInternal split number:{} abort...", inputSplit.getSplitNumber());
            return;
        }

        LOG.info("binlog openInternal split number:{} start...", inputSplit.getSplitNumber());
        LOG.info("binlog config:{}", binlogConfig.toString());

        controller = new MysqlEventParser();
        controller.setConnectionCharset(Charset.forName(binlogConfig.getConnectionCharset()));
        controller.setSlaveId(binlogConfig.getSlaveId());
        controller.setDetectingEnable(binlogConfig.getDetectingEnable());
        controller.setDetectingSQL(binlogConfig.getDetectingSql());
        controller.setMasterInfo(new AuthenticationInfo(new InetSocketAddress(binlogConfig.getHost(), binlogConfig.getPort()), binlogConfig.getUsername(), binlogConfig.getPassword()));
        controller.setEnableTsdb(binlogConfig.getEnableTsdb());
        controller.setDestination("example");
        controller.setParallel(binlogConfig.getParallel());
        controller.setParallelBufferSize(binlogConfig.getBufferSize());
        controller.setParallelThreadSize(binlogConfig.getParallelThreadSize());
        controller.setIsGTIDMode(binlogConfig.getGtidMode());

        controller.setAlarmHandler(new BinlogAlarmHandler(this));

        BinlogEventSink sink = new BinlogEventSink(this);
        sink.setPavingData(binlogConfig.getPavingData());
        binlogEventSink = sink;

        controller.setEventSink(sink);

        controller.setLogPositionManager(new BinlogPositionManager(this));

        EntryPosition startPosition = findStartPosition();
        if (startPosition != null) {
            controller.setMasterPosition(startPosition);
        }

        if (StringUtils.isNotEmpty(binlogConfig.getFilter())) {
            controller.setEventFilter(new AviaterRegexFilter(binlogConfig.getFilter()));
        }

        controller.start();
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        if (binlogEventSink != null) {
            return binlogEventSink.takeEvent();
        }
        LOG.warn("binlog park start");
        LockSupport.park(this);
        LOG.warn("binlog park end...");
        return Row.of();
    }

    @Override
    protected void closeInternal() throws IOException {
        if (controller != null && controller.isStart()) {
            controller.stop();
            controller = null;
            LOG.info("binlog closeInternal..., entryPosition:{}", formatState != null ? formatState.getState() : null);
        }

    }

    private EntryPosition findStartPosition() {
        EntryPosition startPosition = null;
        if (formatState != null && formatState.getState() != null && formatState.getState() instanceof EntryPosition) {
            startPosition = (EntryPosition) formatState.getState();
            checkBinlogFile(startPosition.getJournalName());
        } else if (MapUtils.isNotEmpty(binlogConfig.getStart())) {
            startPosition = new EntryPosition();
            String journalName = (String) binlogConfig.getStart().get("journalName");
            checkBinlogFile(journalName);

            if (StringUtils.isNotEmpty(journalName)) {
                startPosition.setJournalName(journalName);
            }

            startPosition.setTimestamp(MapUtils.getLong(binlogConfig.getStart(), "timestamp"));
            startPosition.setPosition(MapUtils.getLong(binlogConfig.getStart(), "position"));
        }

        return startPosition;
    }

    private void checkBinlogFile(String journalName) {
        if (StringUtils.isNotEmpty(journalName)) {
            if (!new BinlogJournalValidator(binlogConfig.getHost(), binlogConfig.getPort(), binlogConfig.getUsername(), binlogConfig.getPassword()).check(journalName)) {
                throw new IllegalArgumentException("Can't find journalName: " + journalName);
            }
        }
    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    public BinlogConfig getBinlogConfig() {
        return binlogConfig;
    }

    public void setBinlogConfig(BinlogConfig binlogConfig) {
        this.binlogConfig = binlogConfig;
    }

    private String formatTableName(String schemaName, String tableName) {
        StringBuilder stringBuilder = new StringBuilder();
        if (tableName.contains(SCHEMA_SPLIT)) {
            return tableName;
        } else {
            return stringBuilder.append(schemaName).append(SCHEMA_SPLIT).append(tableName).toString();
        }
    }

    private void checkSourceAuthority(Collection<String> tables) {
        try (Connection connection = DbUtil.getConnection(binlogConfig.getJdbcUrl(), binlogConfig.getUsername(), binlogConfig.getPassword())) {
            try (Statement statement = connection.createStatement()) {

                //判断用户是否具有REPLICATION权限 没有的话会直接抛出异常MySQLSyntaxErrorException
                statement.execute(("show master status"));

                List<String> failedTables = new ArrayList<>(tables.size());
                for (String tableName : tables) {
                    try {
                        //判断用户是否具备tableName下的读权限
                        statement.executeQuery(buildAuthorityTemplate(tableName));
                    } catch (SQLException e) {
                        failedTables.add(tableName);
                    }
                }

                if (CollectionUtils.isNotEmpty(failedTables)) {
                    String message = "user【" + binlogConfig.getUsername() + "】is not granted table 【" + Joiner.on(",").join(failedTables) + "】read permission";
                    RuntimeException e = new RuntimeException(message);
                    LOG.error("{}", message, ExceptionUtil.getErrorMessage(e));
                    throw e;
                }
            }
        } catch (SQLException e) {
            String message = " jdbcUrl【" + binlogConfig.getJdbcUrl() + "】 make sure that the database configuration and user 【" + binlogConfig.getUsername() + "】 permissions are correct";
            LOG.error("{}", message, ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException(message, e);
        }
    }

    private String buildAuthorityTemplate(String tableName) {
        return String.format(AUTHORITY_TEMPLATE_SQL, tableName);
    }
}
