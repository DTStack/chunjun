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

    /** worker遍历队列时的步长 */
    private int workerSize;

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
        if (!chamberlain.getTableIdentityFromUnblockQueues().isEmpty()) {
            wakeUp();
        }
    }

    private void wakeUp() {
        // 创建worker
        for (String tableIdentity : chamberlain.getTableIdentityFromUnblockQueues()) {
            Worker worker = new Worker(chamberlain, workerSize, out, tableIdentity);
            workerExecutor.execute(worker);
        }
    }

    public void close() {
        closed.compareAndSet(false, true);
    }
}
