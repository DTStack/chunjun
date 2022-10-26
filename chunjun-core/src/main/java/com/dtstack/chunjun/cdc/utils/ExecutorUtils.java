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

package com.dtstack.chunjun.cdc.utils;

import com.dtstack.chunjun.cdc.exception.LogExceptionHandler;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorUtils {

    public static final int DEFAULT_SINGLE = 1;

    private static final String DEFAULT_NAME_PATTERN = "chunjun-thread-pool-%d";

    private static final boolean DEFAULT_IS_DAEMON = false;

    private static final Thread.UncaughtExceptionHandler DEFAULT_EXCEPTION_HANDLE =
            new LogExceptionHandler();

    private ExecutorUtils() {}

    public static ThreadPoolExecutor threadPoolExecutor(
            int corePoolSize, int maximumPoolSize, int keepAliveTime, int queueCapacity) {
        return threadPoolExecutor(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                queueCapacity,
                DEFAULT_NAME_PATTERN,
                DEFAULT_IS_DAEMON,
                DEFAULT_EXCEPTION_HANDLE);
    }

    public static ThreadPoolExecutor threadPoolExecutor(
            int corePoolSize,
            int maximumPoolSize,
            int keepAliveTime,
            int queueCapacity,
            String namePattern,
            boolean isDaemon,
            Thread.UncaughtExceptionHandler exceptionHandler) {
        BasicThreadFactory threadFactory =
                new BasicThreadFactory.Builder()
                        .namingPattern(namePattern)
                        .uncaughtExceptionHandler(exceptionHandler)
                        .daemon(isDaemon)
                        .build();

        ThreadPoolExecutor threadPoolExecutor =
                new ThreadPoolExecutor(
                        corePoolSize,
                        maximumPoolSize,
                        keepAliveTime,
                        TimeUnit.NANOSECONDS,
                        new LinkedBlockingDeque<>(queueCapacity),
                        threadFactory);

        if (!threadPoolExecutor.isTerminated()) {
            return threadPoolExecutor;
        }

        throw new UnsupportedOperationException(
                "Create thread-executor failed! Name pattern: " + namePattern);
    }

    public static ThreadPoolExecutor singleThreadExecutor() {
        return singleThreadExecutor(
                DEFAULT_NAME_PATTERN, DEFAULT_IS_DAEMON, DEFAULT_EXCEPTION_HANDLE);
    }

    public static ThreadPoolExecutor singleThreadExecutor(String namePattern) {
        return singleThreadExecutor(namePattern, DEFAULT_IS_DAEMON, DEFAULT_EXCEPTION_HANDLE);
    }

    public static ThreadPoolExecutor singleThreadExecutor(
            String namePattern,
            boolean isDaemon,
            Thread.UncaughtExceptionHandler exceptionHandler) {
        BasicThreadFactory threadFactory =
                new BasicThreadFactory.Builder()
                        .namingPattern(namePattern)
                        .uncaughtExceptionHandler(exceptionHandler)
                        .daemon(isDaemon)
                        .build();

        ThreadPoolExecutor threadPoolExecutor =
                new ThreadPoolExecutor(
                        DEFAULT_SINGLE,
                        DEFAULT_SINGLE,
                        0,
                        TimeUnit.NANOSECONDS,
                        new LinkedBlockingDeque<>(DEFAULT_SINGLE),
                        threadFactory);

        if (!threadPoolExecutor.isTerminated()) {
            return threadPoolExecutor;
        }

        throw new UnsupportedOperationException(
                "Create single-thread-executor failed! Name pattern:" + namePattern);
    }

    public static ScheduledThreadPoolExecutor scheduleThreadExecutor(
            int corePoolSize,
            String namePattern,
            boolean isDaemon,
            Thread.UncaughtExceptionHandler exceptionHandler) {
        BasicThreadFactory threadFactory =
                new BasicThreadFactory.Builder()
                        .namingPattern(namePattern)
                        .uncaughtExceptionHandler(exceptionHandler)
                        .daemon(isDaemon)
                        .build();
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(corePoolSize, threadFactory);

        if (!executor.isTerminated()) {
            return executor;
        }

        throw new UnsupportedOperationException(
                "Create schedule-thread-executor failed! Name pattern:" + namePattern);
    }

    public static ScheduledExecutorService scheduleThreadExecutor(
            int corePoolSize, String namePattern) {
        BasicThreadFactory threadFactory =
                new BasicThreadFactory.Builder()
                        .namingPattern(namePattern)
                        .uncaughtExceptionHandler(DEFAULT_EXCEPTION_HANDLE)
                        .daemon(DEFAULT_IS_DAEMON)
                        .build();

        ScheduledExecutorService scheduledExecutorService =
                Executors.newScheduledThreadPool(corePoolSize, threadFactory);

        if (!scheduledExecutorService.isTerminated()) {
            return scheduledExecutorService;
        }

        throw new UnsupportedOperationException("Create schedule-thread-executor failed!");
    }
}
