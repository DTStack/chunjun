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

package com.dtstack.flinkx.socket.reader;

import com.dtstack.flinkx.config.SpeedConfig;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.socket.format.SocketInputFormat;
import com.dtstack.flinkx.util.TelnetUtil;
import com.dtstack.flinkx.util.ValueUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

/** 构建InputFormat
 *
 * @author by kunni@dtstack.com
 */

public class SocketBuilder extends BaseRichInputFormatBuilder {

    protected SocketInputFormat format;

    protected String address;

    private static final int ADDRESS_SPLITS = 2;

    public SocketBuilder(){
        super.format = format = new SocketInputFormat();
    }

    public void setAddress(String address) {
        this.address = address;
        format.setAddress(address);
    }

    public void setCodeC(String codeC){
        format.setCodeC(codeC);
    }

    public void setColumns(ArrayList<String> columns){
        format.setColumns(columns);
    }

    @Override
    protected void checkFormat() {
        SpeedConfig speed = format.getDataTransferConfig().getJob().getSetting().getSpeed();
        StringBuilder sb = new StringBuilder(256);
        if(StringUtils.isBlank(address)){
            sb.append("config error:[address] cannot be blank \n");
        }
        String[] hostPort = org.apache.commons.lang.StringUtils.split(address, ConstantValue.COLON_SYMBOL);
        if(hostPort.length != ADDRESS_SPLITS){
            sb.append("please check your host format \n");
        }
        String host = hostPort[0];
        int port = ValueUtil.getIntegerVal(hostPort[1]);
        try{
            TelnetUtil.telnet(host, port);
        }catch (Exception e){
            sb.append("could not establish connection to ").append(address).append("\n");
        }
        if(speed.getReaderChannel() > 1){
            sb.append("Socket can not support readerChannel bigger than 1, current readerChannel is [")
                    .append(speed.getReaderChannel())
                    .append("];\n");
        }else if(speed.getChannel() > 1){
            sb.append("Socket can not support channel bigger than 1, current channel is [")
                    .append(speed.getChannel())
                    .append("];\n");
        }
        if(sb.length() > 0){
            throw new IllegalArgumentException(sb.toString());
        }
    }
}
