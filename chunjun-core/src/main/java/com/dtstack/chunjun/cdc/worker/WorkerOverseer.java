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

import com.dtstack.chunjun.cdc.QueuesChamberlain;
import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerOverseer implements Runnable, Serializable {

    private static final long serialVersionUID = 177978497091465224L;

    private final transient ThreadPoolExecutor workerExecutor;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final QueuesChamberlain chamberlain;

    private final Collector<RowData> collector;

    /** 记录已经被worker线程获得的chunk */
    private final Set<Integer> chunkSet = new HashSet<>();

    /** worker线程的返回结果 */
    private final Set<Future<Integer>> futureSet = new HashSet<>();

    /** worker遍历队列时的步长 */
    private final int workerSize;

    private Exception exception;

    public WorkerOverseer(
            ThreadPoolExecutor workerExecutor,
            QueuesChamberlain chamberlain,
            Collector<RowData> collector,
            int workerSize) {
        this.workerExecutor = workerExecutor;
        this.chamberlain = chamberlain;
        this.collector = collector;
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
        final int workerNum = workerExecutor.getMaximumPoolSize();
        Set<TableIdentifier> tableIdentities = chamberlain.unblockTableIdentities();
        if (!tableIdentities.isEmpty()) {
            // 创建任务分片
            Chunk[] chunks = ChunkSplitter.createChunk(tableIdentities, workerNum);
            for (Chunk chunk : chunks) {
                Worker worker = new Worker(chamberlain, collector, chunk, workerSize);
                Future<Integer> future = workerExecutor.submit(worker);
                chunkSet.add(chunk.getChunkNum());
                futureSet.add(future);
            }
        }
    }

    public void close() {
        closed.compareAndSet(false, true);
    }

    /** 根据worker的返回结果，移除chunkSet中的记录 */
    private void clear() {
        for (Future<Integer> future : futureSet) {
            try {
                Integer chunkNum = future.get();
                chunkSet.remove(chunkNum);
            } catch (Exception e) {
                closed.compareAndSet(false, true);
                this.exception = e;
            }
        }
        futureSet.clear();
    }

    public boolean isAlive() {
        return null == exception;
    }

    public Exception getException() {
        return exception;
    }
}
