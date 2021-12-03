/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dtstack.flinkx.cdc.worker;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2021/12/2
 *     <p>线程池的创建,一个worker线程一次只处理一张表的队列
 */
public class WorkerManager implements Serializable {

    private transient ThreadPoolExecutor executor;

    private final ConcurrentHashMap<String, Deque<RowData>> unblockQueues;

    private final ConcurrentHashMap<String, Deque<RowData>> blockedQueues;

    public WorkerManager(
            ConcurrentHashMap<String, Deque<RowData>> unblockQueues,
            ConcurrentHashMap<String, Deque<RowData>> blockedQueues) {
        this.unblockQueues = unblockQueues;
        this.blockedQueues = blockedQueues;
    }

    /** 创建线程池 */
    public void open() {
        executor =
                executor == null
                        ? new ThreadPoolExecutor(
                                2,
                                3,
                                0,
                                TimeUnit.NANOSECONDS,
                                new LinkedBlockingDeque<>(2),
                                new BasicThreadFactory.Builder()
                                        .uncaughtExceptionHandler(new WorkerExceptionHandler())
                                        .namingPattern("worker-pool-%d")
                                        .daemon(false)
                                        .build())
                        : executor;
    }

    /** 资源关闭 */
    public void close() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    /** 创建线程并提交 */
    public void work(Collector<RowData> out) {
        if (!unblockQueues.isEmpty()) {
            for (String key : unblockQueues.keySet()) {
                Worker worker = new Worker(unblockQueues, blockedQueues, 3, out, key);
                executor.execute(worker);
            }
        }
    }
}
