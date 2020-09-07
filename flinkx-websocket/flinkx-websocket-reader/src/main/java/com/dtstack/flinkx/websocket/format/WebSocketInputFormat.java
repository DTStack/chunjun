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
import com.dtstack.flinkx.util.RetryUtil;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.java_websocket.WebSocket;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.SynchronousQueue;

/** 读取指定WebSocketUrl中的数据
 * @Company: www.dtstack.com
 * @author kunni
 */

public class WebSocketInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = 1L;

    private String serverUrl;

    private DtWebSocketClient client;

    private String codeC;

    private SynchronousQueue<Row> queue = new SynchronousQueue<>();


    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        try{
            client = new DtWebSocketClient(new URI(serverUrl));
            client.setCodeC(codeC);
            client.setQueue(queue);
            // connect是异步操作
            client.connect();
            // 检测三次连接状态
            RetryUtil.executeWithRetry((Callable<Object>) () -> {
                if(client.getReadyState().equals(WebSocket.READYSTATE.OPEN)){
                    return true;
                }else {
                    throw new RuntimeException("connection not completed");
                }
            }, 3, 1000, false);
        } catch (Exception e) {
            throw new IOException(e.getCause());
        }
        client.run();
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        InputSplit[] inputSplits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new GenericInputSplit(i,minNumSplits);
        }
        return inputSplits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        return null;
    }

    @Override
    protected void closeInternal() throws IOException {
        if(client!=null){
            client.close();
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    public void setServerUrl(String serverUrl){
        this.serverUrl = serverUrl;
    }

    public void setCodeC(String codeC){
        this.codeC = codeC;
    }

}
