/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.cdc.cache;

import org.apache.flink.table.data.RowData;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class Cache {

    private final AtomicBoolean isCached = new AtomicBoolean(false);

    private final AtomicBoolean isFull = new AtomicBoolean(false);

    private LockedBlockingQueue<RowData> out;

    private LockedBlockingQueue<RowData> backup;

    private final int cacheSize;

    public Cache(int cacheSize, long timeout) {
        this.cacheSize = cacheSize;
        this.out = new LockedBlockingQueue<>(cacheSize, timeout);
        this.backup = new LockedBlockingQueue<>(cacheSize, timeout);
    }

    public void add(RowData data) {
        if (out.size() >= cacheSize) {
            isFull.compareAndSet(false, true);
        }

        if (isFull.get()) {
            backup.offer(data);
        } else {
            out.offer(data);
        }
    }

    public boolean backupIsFull() {
        return backup.size() >= cacheSize;
    }

    public void swapToOut() {
        LockedBlockingQueue<RowData> queue = backup;
        backup = out;
        out = queue;
    }

    public RowData oneFromBackup() {
        return backup.peek();
    }

    public Queue<RowData> allFromBackup() {
        return backup.getAll();
    }

    public RowData oneFromOut() {
        try {
            return out.peek();
        } finally {
            if (out.isEmpty()) {
                empty();
            }
        }
    }

    public Queue<RowData> allFromOut() {
        try {
            return out.getAll();
        } finally {
            if (out.isEmpty()) {
                empty();
            }
        }
    }

    public void removeOut(RowData data) {
        out.remove(data);
    }

    public void removeBackup(RowData data) {
        backup.remove(data);
    }

    public boolean isCached() {
        return isCached.get();
    }

    public void cached() {
        isCached.compareAndSet(false, true);
        backup.clear();
    }

    public void uncached() {
        isCached.compareAndSet(true, false);
        backup.clear();
    }

    public void empty() {
        isFull.compareAndSet(true, false);
    }

    public boolean isFull() {
        return isFull.get();
    }
}
