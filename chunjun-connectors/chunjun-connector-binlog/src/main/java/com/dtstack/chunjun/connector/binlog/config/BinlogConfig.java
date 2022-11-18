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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class BinlogConfig extends CommonConfig {

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

    private boolean isUpdrdb = false;

    private List<String> nodeGroupList = new ArrayList<>();

    private List<String> innodbTableNameList = new ArrayList<>();

    private List<String> lamostTableNameList = new ArrayList<>();

    public boolean isUpdrdb() {
        return isUpdrdb;
    }

    public void setUpdrdb(boolean Updrdb) {
        isUpdrdb = Updrdb;
    }

    public List<String> getNodeGroupList() {
        return nodeGroupList;
    }

    public void setDatanodeGroupList(List<String> datanodeGroupList) {
        this.nodeGroupList = datanodeGroupList;
    }

    public List<String> getInnodbTableNameList() {
        return innodbTableNameList;
    }

    public void setInnodbTableNameList(List<String> innodbTableNameList) {
        this.innodbTableNameList = innodbTableNameList;
    }

    public List<String> getLamostTableNameList() {
        return lamostTableNameList;
    }

    public void setLamostTableNameList(List<String> lamostTableNameList) {
        this.lamostTableNameList = lamostTableNameList;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public Map<String, Object> getStart() {
        return start;
    }

    public void setStart(Map<String, Object> start) {
        this.start = start;
    }

    public String getCat() {
        return cat;
    }

    public void setCat(String cat) {
        this.cat = cat;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public boolean isPavingData() {
        return pavingData;
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    public List<String> getTable() {
        return table;
    }

    public void setTable(List<String> table) {
        this.table = table;
    }

    public long getSlaveId() {
        return slaveId;
    }

    public void setSlaveId(long slaveId) {
        this.slaveId = slaveId;
    }

    public String getConnectionCharset() {
        return connectionCharset;
    }

    public void setConnectionCharset(String connectionCharset) {
        this.connectionCharset = connectionCharset;
    }

    public boolean isDetectingEnable() {
        return detectingEnable;
    }

    public void setDetectingEnable(boolean detectingEnable) {
        this.detectingEnable = detectingEnable;
    }

    public String getDetectingSQL() {
        return detectingSQL;
    }

    public void setDetectingSQL(String detectingSQL) {
        this.detectingSQL = detectingSQL;
    }

    public boolean isEnableTsdb() {
        return enableTsdb;
    }

    public void setEnableTsdb(boolean enableTsdb) {
        this.enableTsdb = enableTsdb;
    }

    public boolean isParallel() {
        return parallel;
    }

    public void setParallel(boolean parallel) {
        this.parallel = parallel;
    }

    public int getParallelThreadSize() {
        return parallelThreadSize;
    }

    public void setParallelThreadSize(int parallelThreadSize) {
        this.parallelThreadSize = parallelThreadSize;
    }

    public boolean isGTIDMode() {
        return isGTIDMode;
    }

    public void setGTIDMode(boolean GTIDMode) {
        isGTIDMode = GTIDMode;
    }

    public boolean isSplit() {
        return split;
    }

    public void setSplit(boolean split) {
        this.split = split;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public void setTimestampFormat(String timestampFormat) {
        this.timestampFormat = timestampFormat;
    }

    public int getQueryTimeOut() {
        return queryTimeOut;
    }

    public void setQueryTimeOut(int queryTimeOut) {
        this.queryTimeOut = queryTimeOut;
    }

    public int getConnectTimeOut() {
        return connectTimeOut;
    }

    public void setConnectTimeOut(int connectTimeOut) {
        this.connectTimeOut = connectTimeOut;
    }

    public int getTransactionSize() {
        return transactionSize;
    }

    public boolean isDdlSkip() {
        return ddlSkip;
    }

    public void setDdlSkip(boolean ddlSkip) {
        this.ddlSkip = ddlSkip;
    }

    public boolean isInitialTableStructure() {
        return initialTableStructure;
    }

    public long getOffsetLength() {
        return offsetLength;
    }

    public void setOffsetLength(long offsetLength) {
        this.offsetLength = offsetLength;
    }

    public void setInitialTableStructure(boolean initialTableStructure) {
        this.initialTableStructure = initialTableStructure;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", BinlogConfig.class.getSimpleName() + "[", "]")
                .add("host='" + host + "'")
                .add("port=" + port)
                .add("username='" + username + "'")
                .add("password='" + password + "'")
                .add("jdbcUrl='" + jdbcUrl + "'")
                .add("start=" + start)
                .add("cat='" + cat + "'")
                .add("ddlSkip=" + ddlSkip)
                .add("initialTableStructure=" + initialTableStructure)
                .add("offsetLength=" + offsetLength)
                .add("filter='" + filter + "'")
                .add("period=" + period)
                .add("bufferSize=" + bufferSize)
                .add("transactionSize=" + transactionSize)
                .add("pavingData=" + pavingData)
                .add("table=" + table)
                .add("slaveId=" + slaveId)
                .add("connectionCharset='" + connectionCharset + "'")
                .add("detectingEnable=" + detectingEnable)
                .add("detectingSQL='" + detectingSQL + "'")
                .add("enableTsdb=" + enableTsdb)
                .add("parallel=" + parallel)
                .add("parallelThreadSize=" + parallelThreadSize)
                .add("isGTIDMode=" + isGTIDMode)
                .add("split=" + split)
                .add("timestampFormat='" + timestampFormat + "'")
                .add("queryTimeOut=" + queryTimeOut)
                .add("connectTimeOut=" + connectTimeOut)
                .add("isUpdrdb=" + isUpdrdb)
                .add("nodeGroupList=" + nodeGroupList)
                .add("innodbTableNameList=" + innodbTableNameList)
                .add("lamostTableNameList=" + lamostTableNameList)
                .toString();
    }
}
