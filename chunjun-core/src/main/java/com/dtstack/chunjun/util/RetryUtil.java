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

package com.dtstack.chunjun.util;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.Callable;

@Slf4j
public final class RetryUtil {

    private static final long MAX_SLEEP_MILLISECOND = 256 * 1000L;

    /**
     * 重试次数工具方法.
     *
     * @param callable 实际逻辑
     * @param retryTimes 最大重试次数（>1）
     * @param sleepTimeInMilliSecond 运行失败后休眠对应时间再重试
     * @param exponential 休眠时间是否指数递增
     * @param <T> 返回值类型
     * @return 经过重试的callable的执行结果
     */
    public static <T> T executeWithRetry(
            Callable<T> callable,
            int retryTimes,
            long sleepTimeInMilliSecond,
            boolean exponential) {
        Retry retry = new Retry();
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential, null);
    }

    private static class Retry {

        public <T> T doRetry(
                Callable<T> callable,
                int retryTimes,
                long sleepTimeInMilliSecond,
                boolean exponential,
                List<Class<?>> retryExceptionClazz) {

            if (null == callable) {
                throw new IllegalArgumentException("系统编程错误, 入参callable不能为空 ! ");
            }

            if (retryTimes < 1) {
                throw new IllegalArgumentException(
                        String.format("系统编程错误, 入参retryTime[%d]不能小于1 !", retryTimes));
            }

            Exception saveException = null;
            for (int i = 0; i < retryTimes; i++) {
                try {
                    return call(callable);
                } catch (Exception e) {
                    saveException = e;
                    if (i == 0) {
                        log.error(
                                String.format(
                                        "Exception when calling callable, 异常Msg:%s",
                                        ExceptionUtil.getErrorMessage(saveException)),
                                saveException);
                    }

                    if (null != retryExceptionClazz && !retryExceptionClazz.isEmpty()) {
                        boolean needRetry = false;
                        for (Class<?> eachExceptionClass : retryExceptionClazz) {
                            if (eachExceptionClass == e.getClass()) {
                                needRetry = true;
                                break;
                            }
                        }
                        if (!needRetry) {
                            throw new RuntimeException(saveException);
                        }
                    }

                    if (i + 1 < retryTimes && sleepTimeInMilliSecond > 0) {
                        long startTime = System.currentTimeMillis();

                        long timeToSleep;
                        if (exponential) {
                            timeToSleep = sleepTimeInMilliSecond * (long) Math.pow(2, i);
                        } else {
                            timeToSleep = sleepTimeInMilliSecond;
                        }
                        if (timeToSleep >= MAX_SLEEP_MILLISECOND) {
                            timeToSleep = MAX_SLEEP_MILLISECOND;
                        }

                        try {
                            Thread.sleep(timeToSleep);
                        } catch (InterruptedException ignored) {
                        }

                        long realTimeSleep = System.currentTimeMillis() - startTime;

                        log.error(
                                String.format(
                                        "Exception when calling callable, 即将尝试执行第%s次重试.本次重试计划等待[%s]ms,实际等待[%s]ms, 异常Msg:[%s]",
                                        i + 1,
                                        timeToSleep,
                                        realTimeSleep,
                                        ExceptionUtil.getErrorMessage(e)));
                    }
                }
            }
            throw new RuntimeException(saveException);
        }

        protected <T> T call(Callable<T> callable) throws Exception {
            return callable.call();
        }
    }
}
