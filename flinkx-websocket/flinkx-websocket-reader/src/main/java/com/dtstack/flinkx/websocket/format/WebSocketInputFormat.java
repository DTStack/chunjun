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

package com.dtstack.flinkx.websocket.format;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.concurrent.SynchronousQueue;

import static com.dtstack.flinkx.websocket.constants.WebSocketConfig.KEY_EXIT0;

/** 读取指定WebSocketUrl中的数据
 * @Company: www.dtstack.com
 * @author kunni
 */

public class WebSocketInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = 1L;

    public String serverUrl;

    protected WebSocketClient client;

    /**
     * 重试次数
     */
    protected int retryTime;

    /**
     * 重试间隔
     */
    protected int retryInterval;

    /**
     * 存放数据的队列
     */
    private final SynchronousQueue<Row> queue = new SynchronousQueue<>();


    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        try {
            client = new WebSocketClient(queue, serverUrl)
                    .setRetryInterval(retryInterval)
                    .setRetryTime(retryTime);
            client.run();
        }catch (Exception e){
            throw new IOException(e);
        }
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        InputSplit[] inputSplits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new GenericInputSplit(i,minNumSplits);
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        try {
            row = queue.take();
            // 设置特殊字符串，作为失败标志
            if(StringUtils.equals((CharSequence) row.getField(0), KEY_EXIT0)){
                throw new RuntimeException("webSocket client lost connection completely, job failed.");
            }
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error: {}", ExceptionUtil.getErrorMessage(e));
            throw new IOException(e);
        }
        return row;
    }

    @Override
    protected void closeInternal() {
        if(client != null){
            client.close();
        }
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    public void setServerUrl(String serverUrl){
        this.serverUrl = serverUrl;
    }

    public void setRetryTime(int retryTime){
        this.retryTime = retryTime;
    }

    public void setRetryInterval(int retryInterval){
        this.retryInterval = retryInterval;
    }
}
