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

    private boolean openKerberos;

    private String user;

    private String keytabPath;

    private Integer workerCount;

    private Integer bossCount;

    private Long operationTimeout;

    private Long adminOperationTimeout;


    public boolean getOpenKerberos() {
        return openKerberos;
    }

    public void setOpenKerberos(boolean openKerberos) {
        this.openKerberos = openKerberos;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getKeytabPath() {
        return keytabPath;
    }

    public void setKeytabPath(String keytabPath) {
        this.keytabPath = keytabPath;
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
