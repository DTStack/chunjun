/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.oraclelogminer.listener;

import lombok.Data;

import java.math.BigInteger;

@Data
public class LogFile {

    private String fileName;

    private BigInteger firstChange;

    private BigInteger nextChange;

    private Long thread;

    /**
     * <a href="https://docs.oracle.com/cd/B12037_01/server.101/b10755/dynviews_1132.htm">日志文件状态</a>
     * V$LOGMNR_LOGS里的status
     */
    private int status;

    // 是归档日志 还是online日志
    private String type;

    /** 文件大小 * */
    private Long bytes;

    public boolean isOnline() {
        return "ONLINE".equals(this.type);
    }
}
