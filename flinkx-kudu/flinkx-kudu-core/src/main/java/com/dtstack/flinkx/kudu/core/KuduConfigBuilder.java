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

import org.apache.commons.lang.StringUtils;
import org.apache.flink.util.Preconditions;


/**
 * @author jiangbo
 * @date 2019/8/2
 */
public final class KuduConfigBuilder {
    private String masterAddresses;
    private boolean openKerberos;
    private String user;
    private String keytabPath;
    private Integer workerCount;
    private Integer bossCount;
    private Long operationTimeout;
    private Long adminOperationTimeout;
    private String table;
    private String readMode;
    private String filterString;

    private KuduConfigBuilder() {
    }

    public static KuduConfigBuilder getInstance() {
        return new KuduConfigBuilder();
    }

    public KuduConfigBuilder withMasterAddresses(String masterAddresses) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(masterAddresses), "Parameter [masterAddresses] can not be null or empty");
        this.masterAddresses = masterAddresses;
        return this;
    }

    public KuduConfigBuilder withOpenKerberos(boolean openKerberos) {
        this.openKerberos = openKerberos;
        return this;
    }

    public KuduConfigBuilder withUser(String user) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(user), "Parameter [user] can not be null or empty");
        this.user = user;
        return this;
    }

    public KuduConfigBuilder withKeytabPath(String keytabPath) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(keytabPath), "Parameter [keytabPath] can not be null or empty");
        this.keytabPath = keytabPath;
        return this;
    }

    public KuduConfigBuilder withWorkerCount(Integer workerCount) {
        Preconditions.checkArgument(workerCount > 0, "Parameter [workerCount] should be greater than 0");
        this.workerCount = workerCount;
        return this;
    }

    public KuduConfigBuilder withBossCount(Integer bossCount) {
        Preconditions.checkArgument(bossCount > 0, "Parameter [bossCount] should be greater than 0");
        this.bossCount = bossCount;
        return this;
    }

    public KuduConfigBuilder withOperationTimeout(Long operationTimeout) {
        Preconditions.checkArgument(operationTimeout > 0, "Parameter [operationTimeout] should be greater than 0");
        this.operationTimeout = operationTimeout;
        return this;
    }

    public KuduConfigBuilder withAdminOperationTimeout(Long adminOperationTimeout) {
        Preconditions.checkArgument(adminOperationTimeout > 0, "Parameter [adminOperationTimeout] should be greater than 0");
        this.adminOperationTimeout = adminOperationTimeout;
        return this;
    }

    public KuduConfigBuilder withTable(String table){
        Preconditions.checkArgument(StringUtils.isNotEmpty(table), "Parameter [table] can not be null or empty");
        this.table = table;
        return this;
    }

    public KuduConfigBuilder withReadMode(String readMode){
        Preconditions.checkArgument(StringUtils.isNotEmpty(readMode), "Parameter [readMode] can not be null or empty");
        this.readMode = readMode;
        return this;
    }

    public KuduConfigBuilder withFilter(String filter){
        Preconditions.checkArgument(StringUtils.isNotEmpty(filter), "Parameter [filter] can not be null or empty");
        this.filterString = filter;
        return this;
    }

    public KuduConfig build() {
        KuduConfig kuduConfig = new KuduConfig();
        kuduConfig.setMasterAddresses(masterAddresses);
        kuduConfig.setOpenKerberos(openKerberos);
        kuduConfig.setUser(user);
        kuduConfig.setKeytabPath(keytabPath);
        kuduConfig.setWorkerCount(workerCount);
        kuduConfig.setBossCount(bossCount);
        kuduConfig.setOperationTimeout(operationTimeout);
        kuduConfig.setAdminOperationTimeout(adminOperationTimeout);
        kuduConfig.setTable(table);
        kuduConfig.setReadMode(readMode);
        kuduConfig.setFilterString(filterString);
        return kuduConfig;
    }
}
