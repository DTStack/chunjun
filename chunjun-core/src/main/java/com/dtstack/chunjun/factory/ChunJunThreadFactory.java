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

package com.dtstack.chunjun.factory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ChunJunThreadFactory implements ThreadFactory {
    private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
    private static final AtomicInteger THREAD_NUMBER = new AtomicInteger(1);
    private final ThreadGroup group;
    private final String namePrefix;
    private Boolean isDaemon = false;
    private Thread.UncaughtExceptionHandler uncaughtExceptionHandler;

    public ChunJunThreadFactory(String factoryName) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namePrefix = factoryName + "-pool-" + POOL_NUMBER.getAndIncrement() + "-thread-";
    }

    public ChunJunThreadFactory(String factoryName, Boolean isDaemon) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namePrefix = factoryName + "-pool-" + POOL_NUMBER.getAndIncrement() + "-thread-";
        this.isDaemon = isDaemon;
    }

    public ChunJunThreadFactory(
            String factoryName, Boolean isDaemon, Thread.UncaughtExceptionHandler callback) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namePrefix = factoryName + "-pool-" + POOL_NUMBER.getAndIncrement() + "-thread-";
        this.isDaemon = isDaemon;
        this.uncaughtExceptionHandler = callback;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + THREAD_NUMBER.getAndIncrement(), 0);
        if (this.isDaemon) {
            t.setDaemon(true);
        } else {
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
        }

        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }

        if (uncaughtExceptionHandler != null) {
            t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
        return t;
    }
}
