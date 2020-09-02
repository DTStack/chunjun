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

package com.dtstack.flinkx.websocket.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.websocket.format.WebSocketInputFormat;
import org.apache.commons.lang.StringUtils;

/** 构建 WebSocketInputFormat
 * @Company: www.dtstack.com
 * @author kunni
 */

public class WebSocketInputFormatBuilder extends BaseRichInputFormatBuilder {

    private WebSocketInputFormat format;

    private String serverUrl;

    public static final String WEB_SOCKET_PREFIX = "ws";

    public WebSocketInputFormatBuilder(){
        super.format = format = new WebSocketInputFormat();
    }


    public void setServerUrl(String serverUrl){
        this.serverUrl = serverUrl;
        format.setServerUrl(serverUrl);
    }

    public void setCodeC(String codeC){
        format.setCodeC(codeC);
    }

    @Override
    protected void checkFormat() {
        if(serverUrl==null || !StringUtils.startsWith(serverUrl, WEB_SOCKET_PREFIX)){
            throw new IllegalArgumentException("please check serverUrl");
        }
    }
}
