/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.cdc.monitor.store;

import com.dtstack.chunjun.cdc.QueuesChamberlain;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Deque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/3 星期五
 */
public abstract class StoreBase implements Runnable, Serializable {

    protected QueuesChamberlain chamberlain;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected CopyOnWriteArrayList<String> storedTableIdentifier;

    @Override
    public void run() {
        while (!closed.get()) {
            for (String table : chamberlain.blockTableIdentities()) {
                // 如果数据已经被下发了，那么就跳过
                if (storedTableIdentifier.contains(table)) {
                    continue;
                }
                // 将block的ddl数据下发到外部数据源中
                final Deque<RowData> rowDataDeque = chamberlain.fromBlock(table);
                RowData data = rowDataDeque.peekFirst();
                if (store(data)) {
                    storedTableIdentifier.add(table);
                }
            }
        }
    }

    public void setChamberlain(QueuesChamberlain chamberlain) {
        this.chamberlain = chamberlain;
    }

    public void setStoredTableIdentifier(CopyOnWriteArrayList<String> storedTableIdentifier) {
        this.storedTableIdentifier = storedTableIdentifier;
    }

    public void close() {
        closed.compareAndSet(false, true);
        closeSubclass();
    }

    /**
     * 存储row data.
     *
     * @param data row data
     * @return 是否存储成功
     */
    public abstract boolean store(RowData data);

    /**
     * open sub-class
     *
     * @throws Exception exception
     */
    public abstract void open() throws Exception;

    /** class sub-class. */
    public abstract void closeSubclass();
}
