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
package com.dtstack.chunjun.connector.starrocks.config;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.starrocks.options.ConstantValue;

import org.apache.flink.table.types.DataType;

import com.google.common.collect.Maps;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class StarRocksConfig extends CommonConfig {

    private static final long serialVersionUID = 3755174342122073385L;
    // common
    private String url;

    private List<String> feNodes;

    private String database;

    private String table;

    private String username;

    private String password;

    private String writeMode;
    /** default value is 3 */
    private Integer maxRetries = 3;

    // sink
    /**
     * Whether to save the table structure information obtained from the source for each
     * tableIdentify
     */
    private boolean isCacheTableStruct = true;

    /**
     * In a multi-table write scenario, whether to check the table structure only once for each
     * table.This parameter is valid only when isCacheTableStruct=true
     */
    private boolean checkStructFirstTime = true;

    /** The time to sleep when the tablet version is too large */
    private long waitRetryMills = 18000;

    /** 是否配置了NameMapping, true, RowData中将携带名称匹配后的数据库和表名, sink端配置的database和table失效* */
    private boolean nameMapped;

    private LoadConfig loadConfig =
            LoadConfig.builder()
                    .httpCheckTimeoutMs(ConstantValue.HTTP_CHECK_TIMEOUT_DEFAULT)
                    .queueOfferTimeoutMs(ConstantValue.QUEUE_OFFER_TIMEOUT_DEFAULT)
                    .queuePollTimeoutMs(ConstantValue.QUEUE_POLL_TIMEOUT_DEFAULT)
                    .batchMaxSize(ConstantValue.SINK_BATCH_MAX_BYTES_DEFAULT)
                    .batchMaxRows(ConstantValue.SINK_BATCH_MAX_ROWS_DEFAULT)
                    .headProperties(Maps.newHashMap())
                    .build();

    // source
    private String[] fieldNames;

    private DataType[] dataTypes;

    private String filterStatement;

    private int beClientKeepLiveMin = 10;

    private int beQueryTimeoutSecond = 600;

    private int beClientTimeout = 3000;

    private int beFetchRows = 1024;

    private long beFetchMaxBytes = 1024 * 1024 * 1024;

    private Map<String, String> beSocketProperties = new HashMap<>();

    protected List<String> preSql;
    protected List<String> postSql;
}
