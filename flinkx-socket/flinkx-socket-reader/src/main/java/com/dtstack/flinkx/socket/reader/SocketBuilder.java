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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.socket.format.SocketInputFormat;
import com.dtstack.flinkx.util.TelnetUtil;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;

/** 构建InputFormat
 *
 * @author by kunni@dtstack.com
 */

public class SocketBuilder extends BaseRichInputFormatBuilder {

    protected SocketInputFormat format;

    protected String address;

    public SocketBuilder(){
        super.format = format = new SocketInputFormat();
    }

    public void setAddress(String address) {
        this.address = address;
        format.setAddress(address);
    }

    public void setCodeC(String binaryArrayDecoder){
        format.setCodeC(binaryArrayDecoder);
    }

    public void setColumns(ArrayList<String> columns){
        format.setColumns(columns);
    }

    @Override
    protected void checkFormat() {
        if(StringUtils.isBlank(address) || !StringUtils.contains(address, ConstantValue.COLON_SYMBOL)){
            throw new RuntimeException("please check your [address] = [" + address + "]");
        }

        TelnetUtil.telnet(address);
        // todo channel校验
    }
}
