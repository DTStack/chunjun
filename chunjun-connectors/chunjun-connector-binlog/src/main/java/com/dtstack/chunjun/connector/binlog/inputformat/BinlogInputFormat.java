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

package com.dtstack.chunjun.connector.binlog.inputformat;

import com.dtstack.chunjun.connector.binlog.config.BinlogConfig;
import com.dtstack.chunjun.connector.binlog.listener.BinlogAlarmHandler;
import com.dtstack.chunjun.connector.binlog.listener.BinlogEventSink;
import com.dtstack.chunjun.connector.binlog.listener.BinlogJournalValidator;
import com.dtstack.chunjun.connector.binlog.listener.BinlogPositionManager;
import com.dtstack.chunjun.connector.binlog.listener.HeartBeatController;
import com.dtstack.chunjun.connector.binlog.util.BinlogUtil;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractCDCRawTypeMapper;
import com.dtstack.chunjun.restore.FormatState;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.util.ClassUtil;
import com.dtstack.chunjun.util.JsonUtil;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

@Slf4j
@Getter
@Setter
public class BinlogInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = -8239253671514109268L;

    protected BinlogConfig binlogConfig;
    protected volatile EntryPosition entryPosition;
    protected List<String> categories = new ArrayList<>();
    protected AbstractCDCRawTypeMapper cdcRowConverter;

    protected transient MysqlEventParser controller;
    protected transient BinlogEventSink binlogEventSink;
    protected List<String> tableFilters;

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        log.info(
                "binlog FilterBefore:{}, tableBefore: {}",
                binlogConfig.getFilter(),
                binlogConfig.getTable());
        ClassUtil.forName(BinlogUtil.DRIVER_NAME, getClass().getClassLoader());

        if (StringUtils.isNotEmpty(binlogConfig.getCat()) || !binlogConfig.isDdlSkip()) {
            if (StringUtils.isNotEmpty(binlogConfig.getCat())) {
                categories =
                        Arrays.stream(
                                        binlogConfig
                                                .getCat()
                                                .toUpperCase()
                                                .split(ConstantValue.COMMA_SYMBOL))
                                .collect(Collectors.toList());
            }

            if (!binlogConfig.isDdlSkip()) {
                categories.add(CanalEntry.EventType.CREATE.name());
                categories.add(CanalEntry.EventType.ALTER.name());
                categories.add(CanalEntry.EventType.ERASE.name());
                categories.add(CanalEntry.EventType.TRUNCATE.name());
                categories.add(CanalEntry.EventType.RENAME.name());
                categories.add(CanalEntry.EventType.CINDEX.name());
                categories.add(CanalEntry.EventType.DINDEX.name());
                categories.add(CanalEntry.EventType.QUERY.name());
            }
        }
        /*
         mysql 数据解析关注的表，Perl正则表达式.

        多个正则之间以逗号(,)分隔，转义符需要双斜杠(\\)


        常见例子：

        1.  所有表：.*   or  .*\\..*
        2.  canal schema下所有表： canal\\..*
        3.  canal下的以canal打头的表：canal\\.canal.*
        4.  canal schema下的一张表：canal\\.test1

        5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)
        */
        String jdbcUrl = binlogConfig.getJdbcUrl();
        if (StringUtils.isNotBlank(jdbcUrl)) {
            String database = BinlogUtil.getDataBaseByUrl(jdbcUrl);
            List<String> tables = binlogConfig.getTable();
            if (CollectionUtils.isNotEmpty(tables)) {
                tableFilters =
                        tables.stream()
                                // 每一个表格式化为schema.tableName格式
                                .map(t -> BinlogUtil.formatTableName(database, t))
                                .collect(Collectors.toList());
                String filter = String.join(ConstantValue.COMMA_SYMBOL, tableFilters);

                binlogConfig.setFilter(filter);
            } else if (StringUtils.isBlank(binlogConfig.getFilter())) {
                // 如果table未指定  filter未指定 只消费此schema下的数据
                binlogConfig.setFilter(database + "\\..*");
                tableFilters = new ArrayList<>();
                tableFilters.add(database + "\\..*");
            } else if (StringUtils.isNotBlank(binlogConfig.getFilter())) {
                tableFilters = Arrays.asList(binlogConfig.getFilter().split(","));
            }
            log.info(
                    "binlog FilterAfter:{},tableAfter: {}",
                    binlogConfig.getFilter(),
                    binlogConfig.getTable());
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        if (inputSplit.getSplitNumber() != 0) {
            log.info("binlog openInternal split number:{} abort...", inputSplit.getSplitNumber());
            return;
        }

        log.info("binlog openInternal split number:{} start...", inputSplit.getSplitNumber());
        log.info("binlog config:{}", JsonUtil.toPrintJson(binlogConfig));

        binlogEventSink = new BinlogEventSink(this);
        controller =
                getController(binlogConfig.username, binlogConfig.getFilter(), binlogEventSink);

        // 任务启动前 先初始化表结构
        if (binlogConfig.isInitialTableStructure()) {
            binlogEventSink.initialTableStructData(tableFilters);
        }
        controller.start();
    }

    protected MysqlEventParser getController(
            String username, String filter, BinlogEventSink binlogEventSink) {
        MysqlEventParser controller = new MysqlEventParser();
        controller.setConnectionCharset(
                Charset.forName(binlogConfig.getConnectionCharset()).name());
        controller.setSlaveId(binlogConfig.getSlaveId());
        controller.setDetectingEnable(binlogConfig.isDetectingEnable());
        controller.setDetectingSQL(binlogConfig.getDetectingSQL());
        controller.setMasterInfo(
                new AuthenticationInfo(
                        new InetSocketAddress(binlogConfig.getHost(), binlogConfig.getPort()),
                        username,
                        binlogConfig.getPassword(),
                        BinlogUtil.getDataBaseByUrl(binlogConfig.getJdbcUrl())));
        controller.setEnableTsdb(binlogConfig.isEnableTsdb());
        controller.setDestination("example");
        controller.setParallel(binlogConfig.isParallel());
        controller.setParallelBufferSize(binlogConfig.getBufferSize());
        controller.setParallelThreadSize(binlogConfig.getParallelThreadSize());
        controller.setIsGTIDMode(binlogConfig.isGTIDMode());

        controller.setAlarmHandler(new BinlogAlarmHandler());
        controller.setTransactionSize(binlogConfig.getTransactionSize());

        controller.setEventSink(binlogEventSink);

        controller.setLogPositionManager(new BinlogPositionManager(this));
        // 添加connection心跳回调处理器
        HeartBeatController heartBeatController = new HeartBeatController();
        heartBeatController.setBinlogEventSink(binlogEventSink);
        controller.setHaController(heartBeatController);
        EntryPosition startPosition = findStartPosition();
        if (startPosition != null) {
            controller.setMasterPosition(startPosition);
        }

        if (StringUtils.isNotEmpty(filter)) {
            log.info("binlogFilter最终值：{},current username: {}", filter, username);
            controller.setEventFilter(new AviaterRegexFilter(filter));
        }
        return controller;
    }

    @Override
    public FormatState getFormatState() {
        super.getFormatState();
        if (formatState != null) {
            formatState.setState(entryPosition);
        }
        return formatState;
    }

    @Override
    protected RowData nextRecordInternal(RowData row) {
        if (binlogEventSink != null) {
            return binlogEventSink.takeRowDataFromQueue();
        }
        log.warn("binlog park start");
        LockSupport.park(this);
        log.warn("binlog park end...");
        return null;
    }

    @Override
    protected void closeInternal() {
        if (controller != null && controller.isStart()) {
            controller.stop();
            controller = null;
            log.info(
                    "binlog closeInternal..., entryPosition:{}",
                    formatState != null ? formatState.getState() : null);
        }
    }

    protected EntryPosition findStartPosition() {
        EntryPosition startPosition = null;
        if (formatState != null
                && formatState.getState() != null
                && formatState.getState() instanceof EntryPosition) {
            startPosition = (EntryPosition) formatState.getState();
            checkBinlogFile(startPosition.getJournalName());
        } else if (MapUtils.isNotEmpty(binlogConfig.getStart())) {
            startPosition = new EntryPosition();
            String journalName = (String) binlogConfig.getStart().get("journal-name");
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
            if (!new BinlogJournalValidator(
                            binlogConfig.getHost(),
                            binlogConfig.getPort(),
                            binlogConfig.getUsername(),
                            binlogConfig.getPassword())
                    .check(journalName)) {
                throw new IllegalArgumentException("Can't find journal-name: " + journalName);
            }
        }
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }
}
