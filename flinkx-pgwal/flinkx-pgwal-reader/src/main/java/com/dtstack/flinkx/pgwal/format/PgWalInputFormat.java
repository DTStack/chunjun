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

package com.dtstack.flinkx.pgwal.format;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.pgwal.PgRelicationSlot;
import com.dtstack.flinkx.pgwal.PgWalUtil;
import com.dtstack.flinkx.pgwal.listener.PgWalListener;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.postgresql.jdbc.PgConnection;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

/**
 * Date: 2019/12/13
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PgWalInputFormat extends BaseRichInputFormat {
    protected String username;
    protected String password;
    protected String url;
    protected String databaseName;
    protected boolean pavingData = false;
    protected List<String> tableList;
    protected String cat;
    protected Integer statusInterval;
    protected Long lsn;
    protected String slotName;
    protected boolean allowCreateSlot;
    protected boolean temporary;

    private PgConnection conn;
    private volatile long startLsn;

    private transient BlockingQueue<Map<String, Object>> queue;
    private transient ExecutorService executor;
    private volatile boolean running = false;

    @Override
    public void openInputFormat() throws IOException{
        super.openInputFormat();
        executor = Executors.newFixedThreadPool(1);
        queue = new SynchronousQueue<>(true);
    }

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        if (inputSplit.getSplitNumber() != 0) {
            LOG.info("PgWalInputFormat openInternal split number:{} abort...", inputSplit.getSplitNumber());
            return;
        }
        LOG.info("PgWalInputFormat openInternal split number:{} start...", inputSplit.getSplitNumber());
        try {
            conn = PgWalUtil.getConnection(url, username, password);
            if(StringUtils.isBlank(slotName)){
                slotName = PgWalUtil.SLOT_PRE + jobId;
            }
            PgRelicationSlot availableSlot = PgWalUtil.checkPostgres(conn, allowCreateSlot, slotName, tableList);
            if(availableSlot == null){
                PgWalUtil.createSlot(conn, slotName, temporary);
            }
            if(lsn != 0){
                startLsn = lsn;
            }else if(formatState != null && formatState.getState() != null){
                startLsn = (long)formatState.getState();
            }

            executor.submit(new PgWalListener(this));
            running = true;
        }catch (Exception e){
            LOG.error("PgWalInputFormat open() failed, e = {}", ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException("PgWalInputFormat open() failed, e = " + ExceptionUtil.getErrorMessage(e));
        }
        LOG.info("PgWalInputFormat[{}]open: end", jobName);

    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        try {
            Map<String, Object> map = queue.take();
            if(map.size() == 1){
                throw new IOException((String) map.get("e"));
            }else{
                startLsn = (long) map.get("lsn");
                row = Row.of(map);
            }
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return row;

    }

    @Override
    public FormatState getFormatState() {
        if (!restoreConfig.isRestore()) {
            LOG.info("return null for formatState");
            return null;
        }

        super.getFormatState();
        if (formatState != null) {
            formatState.setState(startLsn);
        }
        return formatState;
    }

    @Override
    protected void closeInternal() throws IOException {
        if (running) {
            executor.shutdownNow();
            running = false;
            LOG.warn("shutdown SqlServerCdcListener......");
        }

    }

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
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

    public void processEvent(Map<String, Object> event) {
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted event:{} error:{}", event, ExceptionUtil.getErrorMessage(e));
        }
    }


    public boolean isPavingData() {
        return pavingData;
    }

    public List<String> getTableList() {
        return tableList;
    }

    public String getCat() {
        return cat;
    }

    public Integer getStatusInterval() {
        return statusInterval;
    }

    public String getSlotName() {
        return slotName;
    }

    public PgConnection getConn() {
        return conn;
    }

    public long getStartLsn() {
        return startLsn;
    }

    public boolean isRunning() {
        return running;
    }
}
