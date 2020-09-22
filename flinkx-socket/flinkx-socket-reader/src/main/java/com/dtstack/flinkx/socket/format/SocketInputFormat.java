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

package com.dtstack.flinkx.socket.format;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.socket.util.DtChannelInitializer;
import com.dtstack.flinkx.socket.util.DtSocketClient;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

/** 读取socket
 *
 * Date: 2019/09/20
 * Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class SocketInputFormat  extends BaseRichInputFormat {

    protected String host;
    protected String port;
    protected String byteBufDecoder;
    protected String binaryArrayDecoder;
    protected Map<String, String> properties;

    protected DtSocketClient client;
    protected transient BlockingQueue<Row> queue = new SynchronousQueue<>();

    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        client = new DtSocketClient(host, port, queue);
        client.setInitializer(new DtChannelInitializer());
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int i) {
        return new InputSplit[0];
    }

    @Override
    protected Row nextRecordInternal(Row row) {
        try {
            row = queue.take();
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return row;
    }

    @Override
    protected void closeInternal() {
        client.close();
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }

    public void setHost(String server) {
        String[] hostPort = StringUtils.split(server, ConstantValue.COLON_SYMBOL);
        if(hostPort.length!=2){
            throw new RuntimeException("please check your host format");
        }
        this.host = hostPort[0];
        this.port = hostPort[1];
    }

    public void setByteBufDecoder(String byteBufDecoder){
        this.byteBufDecoder = byteBufDecoder;
    }

    public void setBinaryArrayDecoder(String binaryArrayDecoder){
        this.binaryArrayDecoder = binaryArrayDecoder;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
