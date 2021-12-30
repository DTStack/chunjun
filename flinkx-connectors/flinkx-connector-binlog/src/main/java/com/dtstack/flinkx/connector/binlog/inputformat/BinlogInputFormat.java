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
package com.dtstack.flinkx.connector.binlog.inputformat;

import com.dtstack.flinkx.connector.binlog.conf.BinlogConf;
import com.dtstack.flinkx.connector.binlog.listener.BinlogAlarmHandler;
import com.dtstack.flinkx.connector.binlog.listener.BinlogEventSink;
import com.dtstack.flinkx.connector.binlog.listener.BinlogJournalValidator;
import com.dtstack.flinkx.connector.binlog.listener.BinlogPositionManager;
import com.dtstack.flinkx.connector.binlog.listener.HeartBeatController;
import com.dtstack.flinkx.connector.binlog.util.BinlogUtil;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.converter.AbstractCDCRowConverter;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.source.format.BaseRichInputFormat;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.JsonUtil;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
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

/** @author toutian */
public class BinlogInputFormat extends BaseRichInputFormat {

    protected BinlogConf binlogConf;
    protected volatile EntryPosition entryPosition;
    protected List<String> categories = new ArrayList<>();
    protected AbstractCDCRowConverter rowConverter;

    protected transient MysqlEventParser controller;
    protected transient BinlogEventSink binlogEventSink;

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
        LOG.info(
                "binlog FilterBefore:{}, tableBefore: {}",
                binlogConf.getFilter(),
                binlogConf.getTable());
        ClassUtil.forName(BinlogUtil.DRIVER_NAME, getClass().getClassLoader());

        if (StringUtils.isNotEmpty(binlogConf.getCat())) {
            LOG.info("{}", categories);
            categories =
                    Arrays.asList(
                            binlogConf.getCat().toUpperCase().split(ConstantValue.COMMA_SYMBOL));
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
        String jdbcUrl = binlogConf.getJdbcUrl();
        if (StringUtils.isNotBlank(jdbcUrl)) {
            String database = BinlogUtil.getDataBaseByUrl(jdbcUrl);
            List<String> tables = binlogConf.getTable();
            if (CollectionUtils.isNotEmpty(tables)) {
                String filter =
                        tables.stream()
                                // 每一个表格式化为schema.tableName格式
                                .map(t -> BinlogUtil.formatTableName(database, t))
                                .collect(Collectors.joining(ConstantValue.COMMA_SYMBOL));

                binlogConf.setFilter(filter);
            } else if (StringUtils.isBlank(binlogConf.getFilter())) {
                // 如果table未指定  filter未指定 只消费此schema下的数据
                binlogConf.setFilter(database + "\\..*");
            }
            LOG.info(
                    "binlog FilterAfter:{},tableAfter: {}",
                    binlogConf.getFilter(),
                    binlogConf.getTable());
        }
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        if (inputSplit.getSplitNumber() != 0) {
            LOG.info("binlog openInternal split number:{} abort...", inputSplit.getSplitNumber());
            return;
        }

        LOG.info("binlog openInternal split number:{} start...", inputSplit.getSplitNumber());
        LOG.info("binlog config:{}", JsonUtil.toPrintJson(binlogConf));

        binlogEventSink = new BinlogEventSink(this);
        controller = getController(binlogConf.username, binlogConf.getFilter(), binlogEventSink);
        controller.start();
    }

    protected MysqlEventParser getController(
            String username, String filter, BinlogEventSink binlogEventSink) {
        MysqlEventParser controller = new MysqlEventParser();
        controller.setConnectionCharset(Charset.forName(binlogConf.getConnectionCharset()));
        controller.setSlaveId(binlogConf.getSlaveId());
        controller.setDetectingEnable(binlogConf.isDetectingEnable());
        controller.setDetectingSQL(binlogConf.getDetectingSQL());
        controller.setMasterInfo(
                new AuthenticationInfo(
                        new InetSocketAddress(binlogConf.getHost(), binlogConf.getPort()),
                        username,
                        binlogConf.getPassword(),
                        BinlogUtil.getDataBaseByUrl(binlogConf.getJdbcUrl())));
        controller.setEnableTsdb(binlogConf.isEnableTsdb());
        controller.setDestination("example");
        controller.setParallel(binlogConf.isParallel());
        controller.setParallelBufferSize(binlogConf.getBufferSize());
        controller.setParallelThreadSize(binlogConf.getParallelThreadSize());
        controller.setIsGTIDMode(binlogConf.isGTIDMode());

        controller.setAlarmHandler(new BinlogAlarmHandler());

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
            LOG.info("binlogFilter最终值：{},current username: {}", filter, username);
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
        LOG.warn("binlog park start");
        LockSupport.park(this);
        LOG.warn("binlog park end...");
        return null;
    }

    @Override
    protected void closeInternal() {
        if (controller != null && controller.isStart()) {
            controller.stop();
            controller = null;
            LOG.info(
                    "binlog closeInternal..., entryPosition:{}",
                    formatState != null ? formatState.getState() : null);
        }
    }

    /**
     * 设置binlog文件起始位置
     *
     * @return
     */
    protected EntryPosition findStartPosition() {
        EntryPosition startPosition = null;
        if (formatState != null
                && formatState.getState() != null
                && formatState.getState() instanceof EntryPosition) {
            startPosition = (EntryPosition) formatState.getState();
            checkBinlogFile(startPosition.getJournalName());
        } else if (MapUtils.isNotEmpty(binlogConf.getStart())) {
            startPosition = new EntryPosition();
            String journalName = (String) binlogConf.getStart().get("journalName");
            checkBinlogFile(journalName);

            if (StringUtils.isNotEmpty(journalName)) {
                startPosition.setJournalName(journalName);
            }

            startPosition.setTimestamp(MapUtils.getLong(binlogConf.getStart(), "timestamp"));
            startPosition.setPosition(MapUtils.getLong(binlogConf.getStart(), "position"));
        }

        return startPosition;
    }

    /**
     * 校验Binlog文件是否存在
     *
     * @param journalName
     */
    private void checkBinlogFile(String journalName) {
        if (StringUtils.isNotEmpty(journalName)) {
            if (!new BinlogJournalValidator(
                            binlogConf.getHost(),
                            binlogConf.getPort(),
                            binlogConf.getUsername(),
                            binlogConf.getPassword())
                    .check(journalName)) {
                throw new IllegalArgumentException("Can't find journalName: " + journalName);
            }
        }
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    public BinlogConf getBinlogConf() {
        return binlogConf;
    }

    public void setBinlogConf(BinlogConf binlogConf) {
        this.binlogConf = binlogConf;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setEntryPosition(EntryPosition entryPosition) {
        this.entryPosition = entryPosition;
    }

    public AbstractCDCRowConverter getRowConverter() {
        return rowConverter;
    }

    public void setRowConverter(AbstractCDCRowConverter rowConverter) {
        this.rowConverter = rowConverter;
    }
}
