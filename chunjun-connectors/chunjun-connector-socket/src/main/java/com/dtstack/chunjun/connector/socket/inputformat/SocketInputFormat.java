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

package com.dtstack.chunjun.connector.socket.inputformat;

import com.dtstack.chunjun.connector.socket.client.DtSocketClient;
import com.dtstack.chunjun.connector.socket.entity.SocketConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.SynchronousQueue;

@Slf4j
public class SocketInputFormat extends BaseRichInputFormat {

    private static final long serialVersionUID = -5069890927073688651L;

    private SocketConfig socketConfig;

    protected DtSocketClient client;

    protected SynchronousQueue<RowData> queue;

    public static final String KEY_EXIT0 = "exit0 ";

    public void setSocketConfig(SocketConfig socketConfig) {
        String[] hostPort =
                StringUtils.split(socketConfig.getAddress(), ConstantValue.COLON_SYMBOL);
        socketConfig.setHost(hostPort[0]);
        socketConfig.setPort(Integer.parseInt(hostPort[1]));
        this.socketConfig = socketConfig;
        super.config = socketConfig;
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
            if (StringUtils.startsWith(
                    String.valueOf(((GenericRowData) row).getField(0)), KEY_EXIT0)) {
                throw new ReadRecordException(
                        "socket client lost connection completely, job failed "
                                + ((GenericRowData) row).getField(0),
                        new Exception("receive data error"));
            }
            row = rowConverter.toInternal(row);
        } catch (InterruptedException e) {
            log.error("takeEvent interrupted error: {}", ExceptionUtil.getErrorMessage(e));
            throw new ReadRecordException(row.toString(), e);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, row);
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
