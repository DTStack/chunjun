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

package com.dtstack.chunjun.cdc;

import com.dtstack.chunjun.cdc.ddl.definition.TableIdentifier;
import com.dtstack.chunjun.cdc.handler.CacheHandler;
import com.dtstack.chunjun.cdc.handler.DDLHandler;

import org.apache.flink.table.data.RowData;

import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** QueuesChamberlain维护blockedQueues、unblockQueues,对外提供访问二者的方法. */
public class QueuesChamberlain implements Serializable {

    private static final long serialVersionUID = 8370483302480527884L;

    private final Lock lock = new ReentrantLock();

    private final CacheHandler cacheHandler;

    private final DDLHandler ddlHandler;

    public QueuesChamberlain(DDLHandler ddlHandler, CacheHandler cacheHandler) {
        this.ddlHandler = ddlHandler;
        this.cacheHandler = cacheHandler;
    }

    public void open() throws Exception {
        ddlHandler.setChamberlain(this);

        ddlHandler.open();
        cacheHandler.open();
    }

    public void close() throws Exception {
        if (null != ddlHandler) {
            ddlHandler.close();
        }

        if (null != cacheHandler) {
            cacheHandler.close();
        }
    }

    public void setCollector(WrapCollector<RowData> wrapCollector) {
        ddlHandler.setCollector(wrapCollector);
    }

    /**
     * 将RowData放入缓存cache中
     *
     * @param data row data.
     * @param tableIdentifier table identifier.
     */
    public void add(RowData data, TableIdentifier tableIdentifier) {
        lock.lock();
        try {
            cacheHandler.add(tableIdentifier, data);
        } finally {
            lock.unlock();
        }
    }

    public void block(List<TableIdentifier> tableIdentifiers) {
        lock.lock();
        try {
            for (TableIdentifier tableIdentifier : tableIdentifiers) {
                if (cacheHandler.isBlock(tableIdentifier)) {
                    return;
                }

                cacheHandler.addNewBlockCache(tableIdentifier);
            }
        } finally {
            lock.unlock();
        }
    }

    public void block(TableIdentifier tableIdentity, RowData rowData) {
        lock.lock();
        try {
            cacheHandler.add(tableIdentity, rowData);
        } finally {
            lock.unlock();
        }
    }

    public void block(TableIdentifier tableIdentity) {
        lock.lock();
        try {
            cacheHandler.block(tableIdentity);
        } finally {
            lock.unlock();
        }
    }

    public void unblock(TableIdentifier tableIdentity) {
        // 将对应的cache置为unblock状态
        lock.lock();
        try {
            cacheHandler.unblock(tableIdentity);
        } finally {
            lock.unlock();
        }
    }

    public RowData dataFromCache(TableIdentifier tableIdentifier) {
        lock.lock();
        try {
            return cacheHandler.get(tableIdentifier);
        } finally {
            lock.unlock();
        }
    }

    public void remove(TableIdentifier tableIdentifier, RowData data) {
        cacheHandler.remove(tableIdentifier, data);
    }

    /** 从unblockQueues中获取所有key集. */
    public Set<TableIdentifier> unblockTableIdentities() {
        return Sets.newHashSet(cacheHandler.getUnblockedTableIdentifiers());
    }

    /** 从blockedQueues中获取所有key集. */
    public Set<TableIdentifier> blockTableIdentities() {
        return Sets.newHashSet(cacheHandler.getBlockedTableIdentifiers());
    }

    public boolean unblockQueuesIsEmpty() {
        return cacheHandler.getUnblockedTableIdentifiers().isEmpty();
    }
}
