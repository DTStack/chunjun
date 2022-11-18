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
package com.dtstack.chunjun.connector.starrocks.conf;

import com.dtstack.chunjun.config.CommonConfig;

import org.apache.flink.table.types.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lihongwei
 * @date 2022/04/11
 */
public class StarRocksConf extends CommonConfig {

    // common
    private String url;

    private List<String> feNodes;

    private String database;

    private String table;

    private String username;

    private String password;

    private String writeMode;
    /** * default value is 3 */
    private Integer maxRetries = 3;

    // sink
    /** The time to sleep when the tablet version is too large */
    private long waitRetryMills = 18000;

    /** 是否配置了NameMapping, true, RowData中将携带名称匹配后的数据库和表名, sink端配置的database和table失效* */
    private boolean nameMapped;

    private LoadConf loadConf = new LoadConf();

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

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<String> getFeNodes() {
        return feNodes;
    }

    public void setFeNodes(List<String> feNodes) {
        this.feNodes = feNodes;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public Integer getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(Integer maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getWaitRetryMills() {
        return waitRetryMills;
    }

    public void setWaitRetryMills(long waitRetryMills) {
        this.waitRetryMills = waitRetryMills;
    }

    public boolean isNameMapped() {
        return nameMapped;
    }

    public void setNameMapped(boolean nameMapped) {
        this.nameMapped = nameMapped;
    }

    public LoadConf getLoadConf() {
        return loadConf;
    }

    public void setLoadConf(LoadConf loadConf) {
        this.loadConf = loadConf;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public DataType[] getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(DataType[] dataTypes) {
        this.dataTypes = dataTypes;
    }

    public String getFilterStatement() {
        return filterStatement;
    }

    public void setFilterStatement(String filterStatement) {
        this.filterStatement = filterStatement;
    }

    public int getBeClientKeepLiveMin() {
        return beClientKeepLiveMin;
    }

    public void setBeClientKeepLiveMin(int beClientKeepLiveMin) {
        this.beClientKeepLiveMin = beClientKeepLiveMin;
    }

    public int getBeQueryTimeoutSecond() {
        return beQueryTimeoutSecond;
    }

    public void setBeQueryTimeoutSecond(int beQueryTimeoutSecond) {
        this.beQueryTimeoutSecond = beQueryTimeoutSecond;
    }

    public int getBeClientTimeout() {
        return beClientTimeout;
    }

    public void setBeClientTimeout(int beClientTimeout) {
        this.beClientTimeout = beClientTimeout;
    }

    public int getBeFetchRows() {
        return beFetchRows;
    }

    public void setBeFetchRows(int beFetchRows) {
        this.beFetchRows = beFetchRows;
    }

    public long getBeFetchMaxBytes() {
        return beFetchMaxBytes;
    }

    public void setBeFetchMaxBytes(long beFetchMaxBytes) {
        this.beFetchMaxBytes = beFetchMaxBytes;
    }

    public Map<String, String> getBeSocketProperties() {
        return beSocketProperties;
    }

    public void setBeSocketProperties(Map<String, String> beSocketProperties) {
        this.beSocketProperties = beSocketProperties;
    }
}
