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
import com.dtstack.chunjun.element.ColumnRowData;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Callable;

/** 下发数据队列中的dml数据，在遇到ddl数据之后，将数据队列的状态置为"block" */
public class Worker implements Callable<Integer> {

    private static final Object LOCK = new Object();
    private final QueuesChamberlain queuesChamberlain;
    private final Collector<RowData> collector;
    /** 任务分片 */
    private final Chunk chunk;
    /** 队列遍历深度，避免某队列长时间占用线程 */
    private final int size;

    public Worker(
            QueuesChamberlain queuesChamberlain,
            Collector<RowData> collector,
            Chunk chunk,
            int size) {
        this.queuesChamberlain = queuesChamberlain;
        this.collector = collector;
        this.chunk = chunk;
        this.size = size;
    }

    /** 发送数据 */
    private void send() {
        Iterator<TableIdentifier> iterator = Arrays.stream(chunk.getTableIdentities()).iterator();
        while (iterator.hasNext()) {
            TableIdentifier tableIdentity = iterator.next();
            for (int i = 0; i < size; i++) {
                RowData data = queuesChamberlain.dataFromCache(tableIdentity);
                if (data == null) {
                    break;
                }

                if (data instanceof ColumnRowData) {
                    dealDmL(data);
                    queuesChamberlain.remove(tableIdentity, data);
                } else {
                    queuesChamberlain.block(tableIdentity);
                    break;
                }
            }
        }
    }

    private void dealDmL(RowData data) {
        synchronized (LOCK) {
            collector.collect(data);
        }
    }

    @Override
    public Integer call() throws Exception {
        send();
        // 返回当前分片的chunkNum给到WorkerOverseer
        return chunk.getChunkNum();
    }
}
