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
package com.dtstack.chunjun.connector.binlog.listener;

import com.dtstack.chunjun.element.ErrorMsgRowData;
import com.dtstack.chunjun.util.ExceptionUtil;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.ha.HeartBeatHAController;
import com.alibaba.otter.canal.parse.inbound.HeartBeatCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartBeatController extends AbstractCanalLifeCycle
        implements CanalHAController, HeartBeatCallback {
    private static final Logger logger = LoggerFactory.getLogger(HeartBeatHAController.class);
    // default 10 times  心跳执行是3秒一次，连续错误3次之后，关闭任务，即宕机后 9s断开连接
    private final int detectingRetryTimes = 3;
    private int failedTimes = 0;
    private BinlogEventSink binlogEventSink;

    public void onSuccess(long costTime) {
        failedTimes = 0;
    }

    @Override
    public void onFailed(Throwable e) {
        failedTimes++;
        // 检查一下是否超过失败次数
        synchronized (this) {
            String msg =
                    String.format(
                            "HeartBeat failed %s times,please check your source is working,error info->%s",
                            failedTimes, ExceptionUtil.getErrorMessage(e));
            logger.error(msg);
            if (failedTimes >= detectingRetryTimes) {
                binlogEventSink.processErrorMsgRowData(new ErrorMsgRowData(msg));
            }
        }
    }

    public void setBinlogEventSink(BinlogEventSink binlogEventSink) {
        this.binlogEventSink = binlogEventSink;
    }
}
