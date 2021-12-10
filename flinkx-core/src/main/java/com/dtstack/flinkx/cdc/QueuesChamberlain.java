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

package com.dtstack.flinkx.cdc;

import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * QueuesChamberlain维护blockedQueues、unblockQueues,对外提供访问二者的方法.
 *
 * @author shitou
 * @date 2021/12/6
 */
public class QueuesChamberlain implements Serializable {

    private static final long serialVersionUID = 2L;

    private final ConcurrentHashMap<String, Deque<RowData>> blockedQueues;

    private final ConcurrentHashMap<String, Deque<RowData>> unblockQueues;

    private final Lock lock = new ReentrantLock();

    public QueuesChamberlain(
            ConcurrentHashMap<String, Deque<RowData>> blockedQueues,
            ConcurrentHashMap<String, Deque<RowData>> unblockQueues) {
        this.blockedQueues = blockedQueues;
        this.unblockQueues = unblockQueues;
    }

    /**
     * 将RowData放入队列中，如果队列中没有对应的数据队列，那么创建一个
     *
     * @param data row data.
     * @param tableIdentifier table identifier.
     */
    public void putRowData(RowData data, String tableIdentifier) {
        lock.lock();
        try {
            if (unblockQueues.containsKey(tableIdentifier)) {
                unblockQueues.get(tableIdentifier).push(data);
            } else if (blockedQueues.containsKey(tableIdentifier)) {
                blockedQueues.get(tableIdentifier).push(data);
            } else {
                // 说明此时不存在该tableIdentifier的数据队列
                Deque<RowData> dataDeque = new LinkedList<>();
                dataDeque.add(data);
                unblockQueues.put(tableIdentifier, dataDeque);
            }
        } finally {
            lock.unlock();
        }
    }

    public void dealDdlRowData(String tableIdentity, Deque<RowData> queue) {
        // 队列头节点是ddl,将该队列放到blockQueues
        lock.lock();
        try {
            blockedQueues.put(tableIdentity, queue);
            unblockQueues.remove(tableIdentity);
        } finally {
            lock.unlock();
        }
    }

    public void dealDmlRowData(String tableIdentity, Deque<RowData> queue) {
        // 队列头节点是dml,将该队列放到unblockQueues
        lock.lock();
        try {
            unblockQueues.put(tableIdentity, queue);
            blockedQueues.remove(tableIdentity);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从unblockQueues中取出表名为tableIdentity的队列
     *
     * @param tableIdentity table identifier.
     */
    public Deque<RowData> getQueueFromUnblockQueues(String tableIdentity) {
        return unblockQueues.get(tableIdentity);
    }

    public Deque<RowData> getQueueFromBlockQueues(String tableIdentity) {
        return blockedQueues.get(tableIdentity);
    }

    /** 从unblockQueues中获取所有key集. */
    public Set<String> getTableIdentitiesFromUnblockQueues() {
        return unblockQueues.keySet();
    }

    /** 从blockedQueues中获取所有key集. */
    public Set<String> getTableIdentitiesFromBlockQueues() {
        return blockedQueues.keySet();
    }

    public boolean unblockQueuesIsEmpty() {
        return unblockQueues.isEmpty();
    }
}
