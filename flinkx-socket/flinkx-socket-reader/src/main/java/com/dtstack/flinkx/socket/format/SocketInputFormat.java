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
import com.dtstack.flinkx.socket.util.DtSocketClient;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.ValueUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.SynchronousQueue;

import static com.dtstack.flinkx.socket.constants.SocketCons.KEY_EXIT0;

/** 读取socket传入的数据
 *
 * @author kunni@dtstack.com
 */

public class SocketInputFormat extends BaseRichInputFormat {

    protected String host;
    protected int port;
    protected String codeC;
    protected ArrayList<String> columns;

    protected DtSocketClient client;
    protected transient SynchronousQueue<Row> queue;

    private static final int ADDR_SPLITS = 2;

    @Override
    protected void openInternal(InputSplit inputSplit) {
        queue = new SynchronousQueue<>();
        client = new DtSocketClient(host, port, queue);
        client.setCodeC(codeC);
        client.start();
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int splitNum) {
        InputSplit[] splits = new InputSplit[splitNum];
        for (int i = 0; i < splitNum; i++) {
            splits[i] = new GenericInputSplit(i,splitNum);
        }

        return splits;
    }

    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        try {
            row = queue.take();
            // 设置特殊字符串，作为失败标志
            if(StringUtils.equals((String) row.getField(0), KEY_EXIT0)){
                throw new RuntimeException("socket client lost connection completely, job failed.");
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

    public void setAddress(String address) {
        String[] hostPort = StringUtils.split(address, ConstantValue.COLON_SYMBOL);
        if(hostPort.length != ADDR_SPLITS){
            throw new RuntimeException("please check your host format");
        }
        this.host = hostPort[0];
        this.port = ValueUtil.getIntegerVal(hostPort[1]);
    }

    public void setCodeC(String codeC){
        this.codeC = codeC;
    }

    public void setColumns(ArrayList<String> columns){
        this.columns = columns;
    }

}
