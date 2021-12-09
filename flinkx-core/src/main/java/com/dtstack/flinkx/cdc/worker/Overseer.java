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

import com.dtstack.flinkx.cdc.QueuesChamberlain;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2021/12/7
 */
public class Overseer implements Runnable, Serializable {

    private static final long serialVersionUID = 2L;

    private final transient ThreadPoolExecutor workerExecutor;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final QueuesChamberlain chamberlain;

    private final Collector<RowData> out;

    /** 记录已经被worker线程获得的tableIdentity */
    private final Set<String> tableSet = new HashSet<>();

    /** worker线程的返回结果 */
    private final List<Future<String>> futureList = new ArrayList<>();

    /** worker遍历队列时的步长 */
    private final int workerSize;

    public Overseer(
            ThreadPoolExecutor workerExecutor,
            QueuesChamberlain chamberlain,
            Collector<RowData> out,
            int workerSize) {
        this.workerExecutor = workerExecutor;
        this.chamberlain = chamberlain;
        this.out = out;
        this.workerSize = workerSize;
    }

    @Override
    public void run() {
        while (!closed.get()) {
            watch();
        }
    }

    /** 监视unblockQueues */
    private void watch() {
        // 判断unblockQueues是否为空，进行遍历操作
        if (!chamberlain.unblockQueuesIsEmpty()) {
            wakeUp();
            clear();
        }
    }

    private void wakeUp() {
        // 创建worker
        for (String tableIdentity : chamberlain.getTableIdentifierFromUnblockQueues()) {
            if (!tableSet.contains(tableIdentity)) {
                tableSet.add(tableIdentity);
                Worker worker = new Worker(chamberlain, workerSize, out, tableIdentity);
                Future<String> future = workerExecutor.submit(worker);
                futureList.add(future);
            }
        }
    }

    public void close() {
        closed.compareAndSet(false, true);
    }

    /** 根据worker的返回结果，移除tableSet中的记录 */
    private void clear() {
        for (Future<String> future : futureList) {
            try {
                String tableIdentity = future.get();
                tableSet.remove(tableIdentity);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
