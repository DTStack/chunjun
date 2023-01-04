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

package com.dtstack.chunjun.connector.binlog.config;

import com.dtstack.chunjun.config.CommonConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class BinlogConfig extends CommonConfig {

    private static final long serialVersionUID = 4689244109824061258L;

    public String host;

    public int port = 3306;

    public String username;

    public String password;

    public String jdbcUrl;

    public Map<String, Object> start;

    public String cat;

    /** 是否支持采集ddl* */
    private boolean ddlSkip = true;

    /** 任务启动时是否初始化表结构* */
    private boolean initialTableStructure = false;

    private long offsetLength = 18;

    public String filter;

    public long period = 1000L;

    public int bufferSize = 256;

    public int transactionSize = 1024;

    public boolean pavingData = true;

    public List<String> table;

    public long slaveId = new Object().hashCode();

    private String connectionCharset = "UTF-8";

    private boolean detectingEnable = true;

    private String detectingSQL = "SELECT CURRENT_DATE";

    private boolean enableTsdb = true;

    private boolean parallel = true;

    private int parallelThreadSize = 2;

    private boolean isGTIDMode;

    private boolean split;

    private String timestampFormat = "sql";

    private int queryTimeOut = 300000;

    private int connectTimeOut = 60000;
}
