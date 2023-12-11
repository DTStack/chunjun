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

import com.dtstack.chunjun.connector.socket.entity.SocketConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;
import com.dtstack.chunjun.util.TelnetUtil;
import com.dtstack.chunjun.util.ValueUtil;

import org.apache.commons.lang3.StringUtils;

public class SocketInputFormatBuilder extends BaseRichInputFormatBuilder<SocketInputFormat> {

    protected SocketConfig socketConfig;

    private static final int ADDRESS_SPLITS = 2;

    public SocketInputFormatBuilder() {
        super(new SocketInputFormat());
    }

    public void setSocketConfig(SocketConfig socketConfig) {
        this.socketConfig = socketConfig;
        format.setSocketConfig(socketConfig);
    }

    @Override
    protected void checkFormat() {
        StringBuilder sb = new StringBuilder(256);
        if (StringUtils.isBlank(socketConfig.getAddress())) {
            sb.append("config error:[address] cannot be blank \n");
        }
        String[] hostPort =
                StringUtils.split(socketConfig.getAddress(), ConstantValue.COLON_SYMBOL);
        if (hostPort.length != ADDRESS_SPLITS) {
            sb.append("please check your host format \n");
        }
        String host = hostPort[0];
        int port = ValueUtil.getIntegerVal(hostPort[1]);
        try {
            TelnetUtil.telnet(host, port);
        } catch (Exception e) {
            sb.append("could not establish connection to ")
                    .append(socketConfig.getAddress())
                    .append("\n");
        }
        if (socketConfig.getParallelism() > 1) {
            sb.append(
                            "Socket can not support readerChannel bigger than 1, current readerChannel is [")
                    .append(socketConfig.getParallelism())
                    .append("];\n");
        }
        if (sb.length() > 0) {
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
