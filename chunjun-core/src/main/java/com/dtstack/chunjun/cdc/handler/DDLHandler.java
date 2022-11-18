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

package com.dtstack.chunjun.cdc.handler;

import com.dtstack.chunjun.cdc.DdlRowData;
import com.dtstack.chunjun.cdc.QueuesChamberlain;
import com.dtstack.chunjun.cdc.WrapCollector;
import com.dtstack.chunjun.cdc.config.DDLConfig;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.cdc.utils.ExecutorUtils;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class DDLHandler implements Serializable {

    private static final long serialVersionUID = 3290050614715459635L;

    protected final List<TableIdentifier> blockedIdentifiers = new ArrayList<>();

    // 定时线程池的间隔时间
    private final DDLConfig ddlConfig;

    protected QueuesChamberlain chamberlain;

    protected WrapCollector<RowData> collector;

    // 定时线程池，用于定时执行轮训外部数据源ddl变更
    private transient ScheduledExecutorService findDDLChangeService;

    private transient ScheduledExecutorService sendDDLChangeService;

    public DDLHandler(DDLConfig ddlConfig) {
        this.ddlConfig = ddlConfig;
    }

    public void setCollector(WrapCollector<RowData> collector) {
        this.collector = collector;
    }

    public void setChamberlain(QueuesChamberlain chamberlain) {
        this.chamberlain = chamberlain;
    }

    public void open() throws Exception {
        Properties properties = ddlConfig.getProperties();

        if (null == properties || properties.isEmpty()) {
            throw new ChunJunRuntimeException(
                    "The 'properties' of 'ddl' item cannot be obtained from the 'restoration'.");
        }

        init(properties);

        // 查询外部数据源中是否存有ddl的数据
        List<TableIdentifier> ddlUnchanged = findDDLUnchanged();
        chamberlain.block(ddlUnchanged);
        blockedIdentifiers.addAll(ddlUnchanged);

        int fetchInterval = ddlConfig.getFetchInterval();
        findDDLChangeService = ExecutorUtils.scheduleThreadExecutor(1, "ddl-change-finder");
        findDDLChangeService.scheduleAtFixedRate(
                this::handleUnblock, fetchInterval, fetchInterval, TimeUnit.MILLISECONDS);

        sendDDLChangeService = ExecutorUtils.scheduleThreadExecutor(1, "ddl-change-sender");
        sendDDLChangeService.scheduleAtFixedRate(
                this::handleBlock, fetchInterval, fetchInterval, TimeUnit.MILLISECONDS);

        log.info("DDL-Handler-finder open succeed! Fetch interval: " + fetchInterval);
    }

    public void close() throws Exception {
        shutdown();

        if (null != findDDLChangeService) {
            findDDLChangeService.shutdown();
        }

        if (null != sendDDLChangeService) {
            sendDDLChangeService.shutdown();
        }
    }

    /** 执行当数据为ddl数据时的操作 */
    public void handleBlock() {
        for (TableIdentifier tableIdentifier : chamberlain.blockTableIdentities()) {
            // 如果数据已经下发过了，则跳过下发该表数据
            if (blockedIdentifiers.contains(tableIdentifier)) {
                continue;
            }

            // 将block的ddl数据下发到外部数据源中
            final RowData data = chamberlain.dataFromCache(tableIdentifier);
            if (null != collector && data != null && insertDDLChange((DdlRowData) data)) {
                // ddl数据需要往下游发送 sink自身判断是否执行ddl语句
                collector.collect(data);
                // 移除队列中已经下发到外部表的ddl数据
                chamberlain.remove(tableIdentifier, data);
                blockedIdentifiers.add(tableIdentifier);
            }
        }
    }

    /** 执行外部数据源中ddl 发生变更之后的操作 */
    public void handleUnblock() {
        List<TableIdentifier> blockChangedIdentifiers = findDDLChanged();
        for (TableIdentifier tableIdentifier : blockChangedIdentifiers) {
            chamberlain.unblock(tableIdentifier);
            blockedIdentifiers.remove(tableIdentifier);
            deleteChangedDDL(tableIdentifier);
        }
    }

    /** 初始化子类 */
    public abstract void init(Properties properties) throws Exception;

    /** 关闭子类 */
    protected abstract void shutdown() throws Exception;

    /**
     * 从外部数据源中查询ddl状态变更
     *
     * @return ddl状态已变更的table-identifiers
     */
    public abstract List<TableIdentifier> findDDLChanged();

    public abstract List<TableIdentifier> findDDLUnchanged();

    /**
     * 下发ddl 数据至外部数据源中
     *
     * @param data ddl 数据.
     */
    public abstract boolean insertDDLChange(DdlRowData data);

    public abstract void updateDDLChange(
            TableIdentifier tableIdentifier,
            String lsn,
            int lsnSequence,
            int status,
            String errorInfo);

    public abstract void deleteChangedDDL(TableIdentifier tableIdentifier);
}
