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


package com.dtstack.flinkx.kudu.core;

import java.io.Serializable;

/**
 * @author jiangbo
 * @date 2019/8/2
 */
public class KuduConfig implements Serializable {

    /**
     * master节点地址:端口，多个以,隔开
     */
    private String masterAddresses;

    /**
     * 认证方式，如:Kerberos
     */
    private String authentication;

    /**
     * 用户名
     */
    private String principal;

    /**
     * keytab文件路径
     */
    private String keytabFile;

    /**
     * worker线程数，默认为cpu*2
     */
    private Integer workerCount;

    /**
     * boss线程数，默认为1
     */
    private Integer bossCount;

    /**
     * 设置普通操作超时时间，默认30S
     */
    private Long operationTimeout;

    /**
     * 设置管理员操作(建表，删表)超时时间，默认30S
     */
    private Long adminOperationTimeout;

    /**
     * 连接scan token的超时时间，如果不设置，则与operationTimeout一致
     */
    private Long queryTimeout;

    /**
     * kudu表名
     */
    private String table;

    /**
     * kudu读取模式：
     *  1、READ_LATEST 默认的读取模式
     *  该模式下，服务器将始终在收到请求时返回已提交的写操作。这种类型的读取不会返回快照时间戳，并且不可重复。
     *  用ACID术语表示，它对应于隔离模式：“读已提交”
     *
     *  2、READ_AT_SNAPSHOT
     *  该模式下，服务器将尝试在提供的时间戳上执行读取。如果未提供时间戳，则服务器将当前时间作为快照时间戳。
     *  在这种模式下，读取是可重复的，即将来所有在相同时间戳记下的读取将产生相同的数据。
     *  执行此操作的代价是等待时间戳小于快照的时间戳的正在进行的正在进行的事务，因此可能会导致延迟损失。用ACID术语，这本身就相当于隔离模式“可重复读取”。
     *  如果对已扫描tablet的所有写入均在外部保持一致，则这对应于隔离模式“严格可序列化”。
     *  注意：当前存在“空洞”，在罕见的边缘条件下会发生，通过这种空洞有时即使在采取措施使写入如此时，它们在外部也不一致。
     *  在这些情况下，隔离可能会退化为“读取已提交”模式。
     *  3、READ_YOUR_WRITES 不支持该模式
     */
    private String readMode;

    /**
     * 过滤条件字符串，如：id >= 1 and time > 1565586665372
     */
    private String filterString;

    /**
     * kudu scan一次性最大读取字节数，默认为1MB
     */
    private int batchSizeBytes;

    /**
     * writer写入时session刷新模式
     *  auto_flush_sync（默认）
     *  auto_flush_background
     *  manual_flush
     */
    private String flushMode;

    public String getFilterString() {
        return filterString;
    }

    public void setFilterString(String filterString) {
        this.filterString = filterString;
    }

    public int getBatchSizeBytes() {
        return batchSizeBytes;
    }

    public void setBatchSizeBytes(int batchSizeBytes) {
        this.batchSizeBytes = batchSizeBytes;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getReadMode() {
        return readMode;
    }

    public void setReadMode(String readMode) {
        this.readMode = readMode;
    }

    public Long getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(Long queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public String getAuthentication() {
        return authentication;
    }

    public void setAuthentication(String authentication) {
        this.authentication = authentication;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getKeytabFile() {
        return keytabFile;
    }

    public void setKeytabFile(String keytabFile) {
        this.keytabFile = keytabFile;
    }

    public Integer getBossCount() {
        return bossCount;
    }

    public void setBossCount(Integer bossCount) {
        this.bossCount = bossCount;
    }

    public String getMasterAddresses() {
        return masterAddresses;
    }

    public void setMasterAddresses(String masterAddresses) {
        this.masterAddresses = masterAddresses;
    }

    public Integer getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(Integer workerCount) {
        this.workerCount = workerCount;
    }

    public Long getOperationTimeout() {
        return operationTimeout;
    }

    public void setOperationTimeout(Long operationTimeout) {
        this.operationTimeout = operationTimeout;
    }

    public Long getAdminOperationTimeout() {
        return adminOperationTimeout;
    }

    public void setAdminOperationTimeout(Long adminOperationTimeout) {
        this.adminOperationTimeout = adminOperationTimeout;
    }

    public String getFlushMode() {
        return flushMode;
    }

    public void setFlushMode(String flushMode) {
        this.flushMode = flushMode;
    }
}
