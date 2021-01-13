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


package com.dtstack.flinkx.oraclelogminer.format;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * @author jiangbo
 * @date 2019/12/14
 */
public class LogMinerConfig implements Serializable {

    private String driverName = "oracle.jdbc.driver.OracleDriver";

    private String jdbcUrl;

    private String username;

    private String password;

    /**
     * LogMiner从v$logmnr_contents视图中批量拉取条数，值越大，消费存量数据越快
     */
    private int fetchSize = 1000;

    private String listenerTables;

    private String cat = "UPDATE,INSERT,DELETE";

    /**
     * 读取位置: all, current, time, scn
     */
    private String readPosition = "current";

    /**
     * 毫秒级时间戳
     */
    private long startTime = 0;

    @JsonProperty("startSCN")
    private String startScn = "";

    private boolean pavingData = false;

    private List<String> table;

    /**
     * LogMiner执行查询SQL的超时参数，单位秒
     */
    private Long queryTimeout = 300L;

    /**
     * Oracle 12c第二个版本之后LogMiner不支持自动添加日志
     */
    private boolean supportAutoAddLog;

    public boolean getSupportAutoAddLog() {
        return supportAutoAddLog;
    }

    public void setSupportAutoAddLog(boolean supportAutoAddLog) {
        this.supportAutoAddLog = supportAutoAddLog;
    }

    public Long getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(Long queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public List<String> getTable() {
        return table;
    }

    public void setTable(List<String> table) {
        this.table = table;
    }

    public String getReadPosition() {
        return readPosition;
    }

    public void setReadPosition(String readPosition) {
        this.readPosition = readPosition;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getStartScn() {
        return startScn;
    }

    public void setStartScn(String startScn) {
        this.startScn = startScn;
    }

    public boolean getPavingData() {
        return pavingData;
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    public String getCat() {
        return cat;
    }

    public void setCat(String cat) {
        this.cat = cat;
    }

    public String getListenerTables() {
        return listenerTables;
    }

    public void setListenerTables(String listenerTables) {
        this.listenerTables = listenerTables;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
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
}
