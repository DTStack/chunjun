/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.cdc.worker;

import com.dtstack.chunjun.cdc.CdcConfig;
import com.dtstack.chunjun.cdc.QueuesChamberlain;
import com.dtstack.chunjun.cdc.exception.LogExceptionHandler;
import com.dtstack.chunjun.cdc.utils.ExecutorUtils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 线程池的创建,管理overseerExecutor和workerExecutor两个线程池,
 *
 * <p>worker线程一次只处理一张表的队列
 */
public class WorkerManager implements Serializable {

    private static final long serialVersionUID = -4853766061454474807L;

    private transient ThreadPoolExecutor workerExecutor;

    private transient ThreadPoolExecutor overseerExecutor;

    private final QueuesChamberlain chamberlain;

    private WorkerOverseer overseer;

    private Collector<RowData> collector;

    /** worker的核心线程数 */
    private final int workerNum;

    /** worker遍历队列时的步长 */
    private final int workerSize;

    /** worker线程池的最大容量 */
    private final int workerMax;

    public WorkerManager(QueuesChamberlain chamberlain, CdcConfig config) {
        this.chamberlain = chamberlain;
        this.workerNum = config.getWorkerNum();
        this.workerSize = config.getWorkerSize();
        this.workerMax = config.getWorkerMax();
    }

    /** 创建线程池 */
    public void open() {
        workerExecutor =
                ExecutorUtils.threadPoolExecutor(
                        workerNum,
                        workerMax,
                        0,
                        workerSize,
                        "worker-pool-%d",
                        true,
                        new LogExceptionHandler());

        overseerExecutor =
                ExecutorUtils.singleThreadExecutor(
                        "overseer-pool-%d", true, new LogExceptionHandler());
    }

    /** 资源关闭 */
    public void close() {
        if (workerExecutor != null) {
            workerExecutor.shutdown();
        }

        if (overseerExecutor != null) {
            if (overseer != null) {
                overseer.close();
            }
            overseerExecutor.shutdown();
        }
    }

    public Collector<RowData> getCollector() {
        return collector;
    }

    public void setCollector(Collector<RowData> collector) {
        this.collector = collector;
        // collector赋值后才能通知Overseer启动worker线程
        openOverseer();
    }

    /** 开启Overseer线程,持续监听unblockQueues */
    private void openOverseer() {
        overseer = new WorkerOverseer(workerExecutor, chamberlain, collector, workerSize);
        overseerExecutor.execute(overseer);
    }

    public boolean isAlive() {
        return overseer.isAlive();
    }

    public Exception getException() {
        return overseer.getException();
    }
}
