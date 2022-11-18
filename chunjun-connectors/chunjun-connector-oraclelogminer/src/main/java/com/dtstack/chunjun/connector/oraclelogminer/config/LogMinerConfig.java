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

package com.dtstack.chunjun.connector.oraclelogminer.config;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.constants.ConstantValue;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Properties;

@EqualsAndHashCode(callSuper = true)
@Data
public class LogMinerConfig extends CommonConfig {

    private static final long serialVersionUID = 7130645155472585510L;

    private String driverName = "oracle.jdbc.driver.OracleDriver";

    private String jdbcUrl;

    private String username;

    private String password;

    /** LogMiner从v$logmnr_contents视图中批量拉取条数，值越大，消费存量数据越快 */
    private int fetchSize = 1000;

    private String listenerTables;

    private String timestampFormat = "sql";

    private String cat = "UPDATE,INSERT,DELETE";

    /** 是否支持采集ddl* */
    private boolean ddlSkip = true;

    /** 任务启动时是否初始化表结构* */
    private boolean initialTableStructure = false;

    /** 读取位置: all, current, time, scn */
    private String readPosition = "current";

    /** 毫秒级时间戳 */
    private long startTime = 0;

    @JsonProperty("startSCN")
    private String startScn = "";

    private boolean pavingData = false;

    private List<String> table;

    /** LogMiner执行查询SQL的超时参数，单位秒 */
    private Long queryTimeout = 300L;

    /** Oracle 12c第二个版本之后LogMiner不支持自动添加日志 */
    private boolean supportAutoAddLog;

    private boolean split;

    /** logminer一次最大加载数据量 默认5g * */
    private long maxLogFileSize = 5 * ConstantValue.STORE_SIZE_G;

    /** 加载日志文件线程个数 * */
    private int ioThreads = 1;

    /** 加载日志文件/查询数据重试次数 * */
    private int retryTimes = 3;

    /** 缓存的日志数 * */
    private long transactionCacheNumSize = 1000;

    /** 每个事务缓存的事件总数 * */
    private long transactionEventSize = 5000;

    private Properties properties;

    /** 缓存的日志时间 * */
    private long transactionExpireTime = 20;

    /** 是否开启全量同步 * */
    private boolean enableFetchAll = false;
}
