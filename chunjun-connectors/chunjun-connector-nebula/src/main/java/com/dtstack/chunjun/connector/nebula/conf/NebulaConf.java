package com.dtstack.chunjun.connector.nebula.conf;
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

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.nebula.utils.NebulaSchemaFamily;
import com.dtstack.chunjun.connector.nebula.utils.WriteMode;

import com.google.common.collect.Lists;
import com.vesoft.nebula.client.graph.data.HostAddress;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.DSTID;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.RANK;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.SRCID;
import static com.dtstack.chunjun.connector.nebula.utils.NebulaConstant.VID;

/**
 * @author: gaoasi
 * @email: aschaser@163.com
 * @date: 2022/10/31 6:20 下午
 */
public class NebulaConf extends CommonConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    /** nebula password */
    private String password;
    /** nebula storage services addr */
    private List<HostAddress> storageAddresses;
    /** nebula graphd services addr */
    private List<HostAddress> graphdAddresses;
    /** nebula connector timeout */
    private Integer timeout;
    /** nebula user name */
    private String username;
    /** should nebula reconnect */
    private Boolean reconn;
    /** nebula graph tag or relation name */
    private String entityName;
    /** nebula graph primery key */
    private String pk;

    /** the number rows each fatch */
    private Integer fetchSize;
    /** the number rows cache each insert */
    private Integer bulkSize;

    /** nebula graph space */
    private String space;

    /** nebula graph schema component: VERTEX TAG EDGE EDGE_TYPE */
    private NebulaSchemaFamily schemaType;

    /** enable nebula SSL connect */
    private Boolean enableSSL;

    /** SSL type: CASignedSSLParam or SelfSignedSSLParam */
    private NebulaSSLParam sslParamType;

    private String caCrtFilePath;
    private String crtFilePath;
    private String keyFilePath;
    private String sslPassword;

    /** retry connect times */
    private Integer connectionRetry;
    /** retry execute times */
    private Integer executionRetry;

    /** the parallelism of read tasks */
    private Integer readTasks;

    /** the parallelism of write tasks */
    private Integer writeTasks;
    /**
     * Pull data within a given time, and set the fetch-interval to achieve the effect of breakpoint
     * resuming, which is equivalent to dividing the time into multiple pull-up data according to
     * the fetch-interval
     */
    private Long interval;
    /** scan the data after the start-time insert */
    private Long start;
    /** scan the data before the end-time insert */
    private Long end;

    private List<String> columnNames;

    private List<FieldConfig> fields;

    /** if allow part success */
    private Boolean defaultAllowPartSuccess;
    /** if allow read from follower */
    private Boolean defaultAllowReadFollower;

    // The min connections in pool for all addresses
    private Integer minConnsSize;

    // The max connections in pool for all addresses
    private Integer maxConnsSize;

    // The idleTime of the connection, unit: millisecond
    // The connection's idle time more than idleTime, it will be delete
    // 0 means never delete
    private Integer idleTime;

    // the interval time to check idle connection, unit ms, -1 means no check
    private Integer intervalIdle;

    // the wait time to get idle connection, unit ms
    private Integer waitTime;

    private WriteMode mode = WriteMode.INSERT;

    private Integer stringLength;

    private String vidType;

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<HostAddress> getStorageAddresses() {
        return storageAddresses;
    }

    public void setStorageAddresses(List<HostAddress> storageAddresses) {
        this.storageAddresses = storageAddresses;
    }

    public List<HostAddress> getGraphdAddresses() {
        return graphdAddresses;
    }

    public void setGraphdAddresses(List<HostAddress> graphdAddresses) {
        this.graphdAddresses = graphdAddresses;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Boolean getReconn() {
        return reconn;
    }

    public void setReconn(Boolean reconn) {
        this.reconn = reconn;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public Integer getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
    }

    public Integer getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(Integer bulkSize) {
        this.bulkSize = bulkSize;
    }

    public String getSpace() {
        return space;
    }

    public void setSpace(String space) {
        this.space = space;
    }

    public NebulaSchemaFamily getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(NebulaSchemaFamily schemaType) {
        this.schemaType = schemaType;
    }

    public Boolean getEnableSSL() {
        return enableSSL;
    }

    public void setEnableSSL(Boolean enableSSL) {
        this.enableSSL = enableSSL;
    }

    public NebulaSSLParam getSslParamType() {
        return sslParamType;
    }

    public void setSslParamType(NebulaSSLParam sslParamType) {
        this.sslParamType = sslParamType;
    }

    public String getCaCrtFilePath() {
        return caCrtFilePath;
    }

    public void setCaCrtFilePath(String caCrtFilePath) {
        this.caCrtFilePath = caCrtFilePath;
    }

    public String getCrtFilePath() {
        return crtFilePath;
    }

    public void setCrtFilePath(String crtFilePath) {
        this.crtFilePath = crtFilePath;
    }

    public String getKeyFilePath() {
        return keyFilePath;
    }

    public void setKeyFilePath(String keyFilePath) {
        this.keyFilePath = keyFilePath;
    }

    public String getSslPassword() {
        return sslPassword;
    }

    public void setSslPassword(String sslPassword) {
        this.sslPassword = sslPassword;
    }

    public Integer getConnectionRetry() {
        return connectionRetry;
    }

    public void setConnectionRetry(Integer connectionRetry) {
        this.connectionRetry = connectionRetry;
    }

    public Integer getExecutionRetry() {
        return executionRetry;
    }

    public void setExecutionRetry(Integer executionRetry) {
        this.executionRetry = executionRetry;
    }

    public Integer getReadTasks() {
        return readTasks;
    }

    public void setReadTasks(Integer readTasks) {
        this.readTasks = readTasks;
    }

    public Integer getWriteTasks() {
        return writeTasks;
    }

    public void setWriteTasks(Integer writeTasks) {
        this.writeTasks = writeTasks;
    }

    public Long getInterval() {
        return interval;
    }

    public void setInterval(Long interval) {
        this.interval = interval;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public Boolean getDefaultAllowPartSuccess() {
        return defaultAllowPartSuccess;
    }

    public void setDefaultAllowPartSuccess(Boolean defaultAllowPartSuccess) {
        this.defaultAllowPartSuccess = defaultAllowPartSuccess;
    }

    public Boolean getDefaultAllowReadFollower() {
        return defaultAllowReadFollower;
    }

    public void setDefaultAllowReadFollower(Boolean defaultAllowReadFollower) {
        this.defaultAllowReadFollower = defaultAllowReadFollower;
    }

    public Integer getMinConnsSize() {
        return minConnsSize;
    }

    public void setMinConnsSize(Integer minConnsSize) {
        this.minConnsSize = minConnsSize;
    }

    public Integer getMaxConnsSize() {
        return maxConnsSize;
    }

    public void setMaxConnsSize(Integer maxConnsSize) {
        this.maxConnsSize = maxConnsSize;
    }

    public Integer getIdleTime() {
        return idleTime;
    }

    public void setIdleTime(Integer idleTime) {
        this.idleTime = idleTime;
    }

    public Integer getIntervalIdle() {
        return intervalIdle;
    }

    public void setIntervalIdle(Integer intervalIdle) {
        this.intervalIdle = intervalIdle;
    }

    public Integer getWaitTime() {
        return waitTime;
    }

    public void setWaitTime(Integer waitTime) {
        this.waitTime = waitTime;
    }

    public WriteMode getMode() {
        return mode;
    }

    public void setMode(WriteMode mode) {
        this.mode = mode;
    }

    public List<FieldConfig> getFields() {
        List<String> var = Lists.newArrayList(VID, SRCID, DSTID, RANK);
        if (fields == null) {
            return null;
        }
        List<FieldConfig> var1 =
                fields.stream()
                        .filter(fieldConf -> !var.contains(fieldConf.getName()))
                        .collect(Collectors.toList());
        return var1;
    }

    public void setFields(List<FieldConfig> fields) {
        this.fields = fields;
    }

    public Integer getStringLength() {
        return stringLength;
    }

    public void setStringLength(Integer stringLength) {
        this.stringLength = stringLength;
    }

    public String getVidType() {
        return vidType;
    }

    public void setVidType(String vidType) {
        this.vidType = vidType;
    }

    @Override
    public String toString() {
        return "NebulaConf{"
                + "password='"
                + password
                + '\''
                + ", timeout="
                + timeout
                + ", username='"
                + username
                + '\''
                + ", reconn="
                + reconn
                + ", entityName='"
                + entityName
                + '\''
                + ", pk='"
                + pk
                + '\''
                + ", fetchSize="
                + fetchSize
                + ", bulkSize="
                + bulkSize
                + ", space='"
                + space
                + '\''
                + ", schemaType="
                + schemaType
                + ", enableSSL="
                + enableSSL
                + ", sslParamType="
                + sslParamType
                + ", caCrtFilePath='"
                + caCrtFilePath
                + '\''
                + ", crtFilePath='"
                + crtFilePath
                + '\''
                + ", keyFilePath='"
                + keyFilePath
                + '\''
                + ", sslPassword='"
                + sslPassword
                + '\''
                + ", connectionRetry="
                + connectionRetry
                + ", executionRetry="
                + executionRetry
                + ", readTasks="
                + readTasks
                + ", writeTasks="
                + writeTasks
                + ", interval="
                + interval
                + ", start="
                + start
                + ", end="
                + end
                + ", columnNames="
                + columnNames
                + ", fields="
                + fields
                + ", defaultAllowPartSuccess="
                + defaultAllowPartSuccess
                + ", defaultAllowReadFollower="
                + defaultAllowReadFollower
                + ", minConnsSize="
                + minConnsSize
                + ", maxConnsSize="
                + maxConnsSize
                + ", idleTime="
                + idleTime
                + ", intervalIdle="
                + intervalIdle
                + ", waitTime="
                + waitTime
                + ", mode="
                + mode
                + ", stringLength="
                + stringLength
                + ", vidType='"
                + vidType
                + '\''
                + '}';
    }
}
