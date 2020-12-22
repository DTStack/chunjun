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
package com.dtstack.flinkx.kafkabase.writer;

import com.dtstack.flinkx.exception.DataSourceException;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HeartBeatController
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/11
 */
public class HeartBeatController implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(HeartBeatController.class);
    private int detectingRetryTimes = 3;
    private AtomicInteger failedTimes = new AtomicInteger(0);
    private Throwable e;

    public HeartBeatController() {

    }

    public HeartBeatController(int detectingRetryTimes, AtomicInteger failedTimes) {
        this.detectingRetryTimes = detectingRetryTimes;
        this.failedTimes = failedTimes;
    }

    public void onSuccess() {
        failedTimes.set(0);
        this.e=null;
    }

    public void onFailed(Throwable e) {
        failedTimes.incrementAndGet();
        this.e = e;

    }

    public void acquire() {
        if (Objects.isNull(e)) {
            return;
        }
        //连续发送3次数据错误
        if (failedTimes.get() >= detectingRetryTimes ) {
            String message = "Failed to send data for three consecutive times，Please check whether the data source is normal，errorInfo->" + ExceptionUtil.getErrorMessage(e);
            logger.error(message);
            throw new DataSourceException("kafka",message, e);
        }
    }
}

