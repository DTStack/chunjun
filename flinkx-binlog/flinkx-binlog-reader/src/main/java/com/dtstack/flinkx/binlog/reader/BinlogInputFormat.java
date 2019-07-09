/**
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
package com.dtstack.flinkx.binlog.reader;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.dtstack.flinkx.binlog.BinlogJournalValidator;
import com.dtstack.flinkx.binlog.BinlogPosUtil;
import com.dtstack.flinkx.inputformat.RichInputFormat;
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

    private Map<String,Object> start;

    private String filter;

    private String cat;

    private long period;

    private int bufferSize;

    /** internal fields */

    private transient MysqlEventParser controller;

    private transient ScheduledExecutorService scheduler;

    private volatile EntryPosition entryPosition = new EntryPosition();

    private List<String> categories = new ArrayList<>();

    private transient BinlogEventSink binlogEventSink;

    public void updateLastPos(EntryPosition entryPosition) {
        this.entryPosition = entryPosition;
    }

    public boolean accept(String type) {
        return categories.isEmpty() || categories.contains(type);
    }

    private void savePos(){
        try {
            BinlogPosUtil.savePos("default_task_id_output", entryPosition);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        LOG.debug("save pos to local, entryPosition:{}", entryPosition);
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        LOG.info("binlog emit started...");

        controller.start();

        LOG.info("binlog scheduleAtFixedRate....");
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                savePos();
            }
        }, period, period, TimeUnit.MILLISECONDS);

        LOG.info("binlog emit ended...");
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return binlogEventSink.takeEvent();
    }

    @Override
    protected void closeInternal() throws IOException {
        LOG.info("binlog release...");

        if(controller != null) {
            controller.stop();
        }

        if(scheduler != null) {
            scheduler.shutdown();
        }

        savePos();
        LOG.info("binlog release..., save pos:{}", entryPosition);
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
            binlogEventSink = new BinlogEventSink(this);
            controller.setEventSink(binlogEventSink);

            controller.setLogPositionManager(new BinlogPositionManager(this));

            EntryPosition startPosition = findStartPosition();
            if (startPosition != null) {
                controller.setMasterPosition(startPosition);
            }

            if (filter != null) {
                controller.setEventFilter(new AviaterRegexFilter(filter));
            }

            LOG.info("binlog prepare ended..");
        } catch (Exception e) {
            LOG.error("",e);
            System.exit(-1);
        }
    }

    private void parseCategories() {
        if(!StringUtils.isBlank(cat)) {
            LOG.info("{}", categories);
            categories = Arrays.asList(cat.toUpperCase().split(","));
        }
    }

    private EntryPosition findStartPosition() {
        if(start != null && start.size() != 0) {
            EntryPosition startPosition = new EntryPosition();
            String journalName = (String) start.get("journalName");
            if(StringUtils.isNotEmpty(journalName)) {
                if(new BinlogJournalValidator(host, port, username, password).check(journalName)) {
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
        try {
            startPosition = BinlogPosUtil.readPos("default_task_id");
        } catch(IOException e) {
            LOG.error("Failed to read pos file: " + e.getMessage());
        }
        return startPosition;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        InputSplit[] split = new InputSplit[1];
        split[0] = new GenericInputSplit(0,1);
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
}
