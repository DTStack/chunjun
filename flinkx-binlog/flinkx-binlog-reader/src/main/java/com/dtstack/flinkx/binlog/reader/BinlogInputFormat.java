/**
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
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.restore.FormatState;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class BinlogInputFormat extends RichInputFormat {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogInputFormat.class);

    private String host;

    private int port;

    private String username;

    private String password;

    private String jdbcUrl;

    private boolean pavingData = false;

    private Map<String, Object> start;

    private List<String> table;

    private String filter;

    private String cat;

    private long period;

    private int bufferSize;

    private volatile EntryPosition entryPosition;

    private List<String> categories = new ArrayList<>();

    /**
     * internal fields
     */

    private transient MysqlEventParser controller;

    private transient BinlogEventSink binlogEventSink;

    public void updateLastPos(EntryPosition entryPosition) {
        formatState.setState(entryPosition);

        if (LOG.isDebugEnabled()) {
            LOG.debug("save pos to local, entryPosition:{}", entryPosition);
        }
    }

    public boolean accept(String type) {
        return categories.isEmpty() || categories.contains(type);
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("binlog emit started...");

        controller.start();
        LOG.info("binlog controller start...");
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return binlogEventSink.takeEvent();
    }

    @Override
    protected void closeInternal() throws IOException {
        LOG.info("binlog release...");

        if (controller != null) {
            controller.stop();
        }

        LOG.info("binlog release..., entryPosition:{}", entryPosition);
    }

    @Override
    public void configure(Configuration parameters) {
        try {
            LOG.info("binlog prepare started..");

            parseCategories();

            controller = new MysqlEventParser();
            controller.setConnectionCharset(Charset.forName("UTF-8"));
            controller.setSlaveId(3344L);
            controller.setDetectingEnable(false);
            controller.setMasterInfo(new AuthenticationInfo(new InetSocketAddress(host, port), username, password));
            controller.setEnableTsdb(true);
            controller.setDestination("example");
            controller.setParallel(true);
            controller.setParallelBufferSize(bufferSize);
            controller.setParallelThreadSize(2);
            controller.setIsGTIDMode(false);

            controller.setAlarmHandler(new BinlogAlarmHandler(this));

            BinlogEventSink sink = new BinlogEventSink(this);
            sink.setPavingData(pavingData);
            binlogEventSink = sink;

            controller.setEventSink(sink);

            controller.setLogPositionManager(new BinlogPositionManager(this));

            EntryPosition startPosition = findStartPosition();
            if (startPosition != null) {
                controller.setMasterPosition(startPosition);
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
            if (table != null && table.size() != 0 && jdbcUrl != null) {
                int idx = jdbcUrl.lastIndexOf('?');
                String database = null;
                if (idx != -1) {
                    database = StringUtils.substring(jdbcUrl, jdbcUrl.lastIndexOf('/') + 1, idx);
                } else {
                    database = StringUtils.substring(jdbcUrl, jdbcUrl.lastIndexOf('/') + 1);
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < table.size(); i++) {
                    sb.append(database).append(".").append(table.get(i));
                    if (i != table.size() - 1) {
                        sb.append(",");
                    }
                }
                filter = sb.toString();
            }

            if (filter != null) {
                controller.setEventFilter(new AviaterRegexFilter(filter));
            }

            LOG.info("binlog prepare ended..");
        } catch (Exception e) {
            LOG.error("", e);
            System.exit(-1);
        }
    }

    private void parseCategories() {
        if (!StringUtils.isBlank(cat)) {
            LOG.info("{}", categories);
            categories = Arrays.asList(cat.toUpperCase().split(","));
        }
    }

    private EntryPosition findStartPosition() {
        if (start != null && start.size() != 0) {
            EntryPosition startPosition = new EntryPosition();
            String journalName = (String) start.get("journalName");
            if (StringUtils.isNotEmpty(journalName)) {
                if (new BinlogJournalValidator(host, port, username, password).check(journalName)) {
                    startPosition.setJournalName(journalName);
                } else {
                    throw new IllegalArgumentException("Can't find journalName: " + journalName);
                }
            }
            startPosition.setTimestamp((Long) start.get("timestamp"));
            startPosition.setPosition((Long) start.get("position"));
            return startPosition;
        }

        EntryPosition startPosition = null;
        if (formatState != null && formatState.getState() != null) {
            startPosition = (EntryPosition) formatState.getState();
        }
        return startPosition;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        InputSplit[] split = new InputSplit[1];
        split[0] = new GenericInputSplit(0, 1);
        return split;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setStart(Map<String, Object> start) {
        this.start = start;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public void setCat(String cat) {
        this.cat = cat;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    public void setTable(List<String> table) {
        this.table = table;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }
}
