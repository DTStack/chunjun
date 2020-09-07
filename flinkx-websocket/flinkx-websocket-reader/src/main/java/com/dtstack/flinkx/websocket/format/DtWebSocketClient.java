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

import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.flink.types.Row;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;

/** 自定义的WebSocket Client
 * @Company: www.dtstack.com
 * @author kunni
 */

public class DtWebSocketClient extends WebSocketClient {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    private String codeC;

    private SynchronousQueue<Row> queue;

    public DtWebSocketClient(URI serverURI) {
        super(serverURI);
    }

    @Override
    public void onOpen(ServerHandshake serverHandshake) {
        LOG.info("connected to URI : " + getURI());
    }

    @Override
    public void onMessage(String s) {
        try{
            Class<?> clazz = Class.forName(codeC);
            Method method = clazz.getMethod("decode",String.class);
            Map<String, Object> map = (Map<String, Object>) method.invoke(clazz.newInstance(), s);
            Row row = new Row(map.size());
            int count = 0;
            for(Map.Entry<String, Object> entry : map.entrySet()){
                row.setField(count++, entry.getValue());
            }
            queue.put(row);
        }catch (Exception e){
            LOG.error("Class no found");
        }
    }

    @Override
    public void onClose(int i, String s, boolean b) {

    }

    @Override
    public void onError(Exception e) {
        LOG.error(ExceptionUtil.getErrorMessage(e));
    }

    public void setCodeC(String codeC){
        this.codeC = codeC;
    }

    public void setQueue(SynchronousQueue<Row> queue){
        this.queue = queue;
    }

}
