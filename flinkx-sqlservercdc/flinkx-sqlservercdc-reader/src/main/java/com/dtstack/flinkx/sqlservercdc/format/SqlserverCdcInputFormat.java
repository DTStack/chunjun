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
package com.dtstack.flinkx.sqlservercdc.format;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.sqlservercdc.Lsn;
import com.dtstack.flinkx.sqlservercdc.SqlServerCdcUtil;
import com.dtstack.flinkx.sqlservercdc.TxLogPosition;
import com.dtstack.flinkx.sqlservercdc.listener.SqlServerCdcListener;
import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.dtstack.flinkx.sqlservercdc.SqlServerCdcUtil.DRIVER;

/**
 * Date: 2019/12/03
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class SqlserverCdcInputFormat extends BaseRichInputFormat {
    protected String username;
    protected String password;
    protected String url;
    protected String databaseName;
    protected boolean pavingData = false;
    protected List<String> tableList;
    protected String cat;
    protected long pollInterval;
    protected String lsn;

    private Connection conn;
    private TxLogPosition logPosition;

    private transient BlockingQueue<Map<String, Object>> queue;
    private transient ExecutorService executor;
    private volatile boolean running = false;

    @Override
    protected void openInternal(InputSplit inputSplit) {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("cdcListener-pool-%d").build();
        executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        queue = new SynchronousQueue<>(false);

        if (inputSplit.getSplitNumber() != 0) {
            LOG.info("sqlServer cdc openInternal split number:{} abort...", inputSplit.getSplitNumber());
            return;
        }

        LOG.info("sqlServer cdc openInternal split number:{} start...", inputSplit.getSplitNumber());
        try {
            ClassUtil.forName(DRIVER, getClass().getClassLoader());
            conn = SqlServerCdcUtil.getConnection(url, username, password);
            conn.setAutoCommit(false);
            SqlServerCdcUtil.changeDatabase(conn, databaseName);

            if(StringUtils.isNotBlank(lsn)){
                logPosition = TxLogPosition.valueOf(Lsn.valueOf(lsn));
            }else if(formatState != null && formatState.getState() != null){
                logPosition = (TxLogPosition)formatState.getState();
            }else{
                logPosition = TxLogPosition.valueOf(SqlServerCdcUtil.getMaxLsn(conn));
            }

            executor.submit(new SqlServerCdcListener(this));
            running = true;
        } catch (Exception e) {
            LOG.error("SqlserverCdcInputFormat open() failed, e = {}", ExceptionUtil.getErrorMessage(e));
            throw new RuntimeException("SqlserverCdcInputFormat open() failed, e = " + ExceptionUtil.getErrorMessage(e));
        }

        LOG.info("SqlserverCdcInputFormat[{}]open: end", jobName);
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        try {
            Map<String, Object> map = queue.take();
            if(map.size() == 1){
                throw new IOException((String) map.get("e"));
            }else{
                row = Row.of(map);
            }
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return row;
    }

    @Override
    protected void closeInternal(){
        if (running) {
            executor.shutdownNow();
            running = false;
            LOG.warn("shutdown SqlServerCdcListener......");
        }
    }


    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    @Override
    public FormatState getFormatState() {
        if (!restoreConfig.isRestore()) {
            LOG.info("return null for formatState");
            return null;
        }

        super.getFormatState();
        if (formatState != null) {
            formatState.setState(logPosition);
        }
        return formatState;
    }

    public void processEvent(Map<String, Object> event) {
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted event:{} error:{}", event, ExceptionUtil.getErrorMessage(e));
        }
    }

    public String getDatabaseName() {
        return databaseName;
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

    public long getPollInterval() {
        return pollInterval;
    }

    public Connection getConn() {
        return conn;
    }

    public void setLogPosition(TxLogPosition logPosition) {
        this.logPosition = logPosition;
    }

    public TxLogPosition getLogPosition() {
        return logPosition;
    }
}
