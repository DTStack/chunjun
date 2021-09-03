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

package com.dtstack.flinkx.connector.socket.inputformat;

import com.dtstack.flinkx.connector.socket.client.DtSocketClient;
import com.dtstack.flinkx.connector.socket.entity.SocketConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.source.format.BaseRichInputFormat;
import com.dtstack.flinkx.throwable.ReadRecordException;
import com.dtstack.flinkx.util.ExceptionUtil;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang.StringUtils;

import java.util.concurrent.SynchronousQueue;

/**
 * 读取socket传入的数据
 *
 * @author kunni@dtstack.com
 */
public class SocketInputFormat extends BaseRichInputFormat {

    private SocketConfig socketConfig;

    protected DtSocketClient client;
    protected SynchronousQueue<RowData> queue;

    public static final String KEY_EXIT0 = "exit0 ";

    public void setSocketConfig(SocketConfig socketConfig) {
        String[] hostPort =
                org.apache.commons.lang.StringUtils.split(
                        socketConfig.getAddress(), ConstantValue.COLON_SYMBOL);
        socketConfig.setHost(hostPort[0]);
        socketConfig.setPort(Integer.parseInt(hostPort[1]));
        this.socketConfig = socketConfig;
        super.config = socketConfig;
    }

    public SocketConfig getSocketConfig() {
        return socketConfig;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        queue = new SynchronousQueue<>();
        client = new DtSocketClient(socketConfig.getHost(), socketConfig.getPort(), queue);
        client.setCodeC(socketConfig.getParse());
        client.setEncoding(socketConfig.getEncoding());
        client.start();
    }

    @Override
    protected InputSplit[] createInputSplitsInternal(int splitNum) {
        InputSplit[] splits = new InputSplit[splitNum];
        for (int i = 0; i < splitNum; i++) {
            splits[i] = new GenericInputSplit(i, splitNum);
        }

        return splits;
    }

    @Override
    protected RowData nextRecordInternal(RowData row) throws ReadRecordException {
        try {
            row = queue.take();
            // 设置特殊字符串，作为失败标志
            if (StringUtils.startsWith((String) ((GenericRowData) row).getField(0), KEY_EXIT0)) {
                throw new ReadRecordException(
                        "socket client lost connection completely, job failed "
                                + ((GenericRowData) row).getField(0),
                        new Exception("receive data error"));
            }
        } catch (InterruptedException e) {
            LOG.error("takeEvent interrupted error: {}", ExceptionUtil.getErrorMessage(e));
            throw new ReadRecordException(row.toString(), e);
        }
        return row;
    }

    @Override
    protected void closeInternal() {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public boolean reachedEnd() {
        return false;
    }
}
