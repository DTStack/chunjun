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

import com.dtstack.flinkx.element.ColumnRowData;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/1 星期三
 *     <p>下发数据队列中的dml数据，在遇到ddl数据之后，将数据队列的状态置为"block"
 */
public class Worker implements Runnable {

    private final ConcurrentHashMap<String, Deque<RowData>> unblockQueues;

    private final ConcurrentHashMap<String, Deque<RowData>> blockedQueues;

    /** 队列遍历深度，避免某队列长时间占用线程 */
    private final int depth;

    private final Collector<RowData> out;

    /** 表标识 */
    private final String tableIdentity;

    public Worker(
            ConcurrentHashMap<String, Deque<RowData>> unblockQueues,
            ConcurrentHashMap<String, Deque<RowData>> blockedQueues,
            int depth,
            Collector<RowData> out,
            String tableIdentity) {
        this.unblockQueues = unblockQueues;
        this.blockedQueues = blockedQueues;
        this.depth = depth;
        this.out = out;
        this.tableIdentity = tableIdentity;
    }

    @Override
    public void run() {
        send();
    }

    /** 发送数据 */
    private void send() {
        Deque<RowData> queue = unblockQueues.get(tableIdentity);
        for (int i = 0; i < depth; i++) {
            RowData data = queue.peek();
            if (data == null) {
                break;
            }

            if (data instanceof ColumnRowData) {
                dealDML(queue);
            } else {
                dealDDL(queue);
            }
        }
    }

    private void dealDML(Deque<RowData> queue) {
        // 队列头节点是dml, 将该dml数据发送到sink
        RowData rowData = queue.poll();
        out.collect(rowData);
    }

    private void dealDDL(Deque<RowData> queue) {
        // 队列头节点是ddl,将该队列放到blockQueues
        blockedQueues.put(tableIdentity, queue);
        unblockQueues.remove(tableIdentity);
    }
}
