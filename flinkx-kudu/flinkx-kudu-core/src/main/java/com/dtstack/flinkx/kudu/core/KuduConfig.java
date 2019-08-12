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

    private String masterAddresses;

    private String authentication;

    private String principal;

    private String keytabFile;

    private Integer workerCount;

    private Integer bossCount;

    private Long operationTimeout;

    private Long adminOperationTimeout;

    private Long queryTimeout;

    private String table;

    private String readMode;

    private String filterString;

    private int batchSizeBytes;

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
}
