/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.connector.kudu.conf;

import com.dtstack.flinkx.conf.FlinkxCommonConf;

import org.apache.flink.configuration.ReadableConfig;

import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.ADMIN_OPERATION_TIMEOUT;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.AUTHENTICATION;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.ENABLE_KRB;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.KEYTAB_FILE;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.MASTER_ADDRESS;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.OPERATION_TIMEOUT;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.PRINCIPAL;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.QUERY_TIMEOUT;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.SCAN_PARALLELISM;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.TABLE_NAME;
import static com.dtstack.flinkx.connector.kudu.options.KuduOptions.WORKER_COUNT;

/**
 * @author tiezhu
 * @since 2021/6/9 星期三
 */
public class KuduCommonConf extends FlinkxCommonConf {

    /** master节点地址:端口，多个以,隔开 */
    protected String masters;

    /** kudu表名 */
    protected String table;

    /** 认证方式，如:Kerberos */
    protected String authentication;

    protected boolean enableKrb;

    /** 用户名 */
    protected String principal;

    /** keytab文件路径 */
    protected String keytabFile;

    /** worker线程数，默认为cpu*2 */
    protected Integer workerCount;

    /** 设置普通操作超时时间，默认30S */
    protected Long operationTimeout;

    /** 设置管理员操作(建表，删表)超时时间，默认30S */
    protected Long adminOperationTimeout;

    /** 连接scan token的超时时间，如果不设置，则与operationTimeout一致 */
    protected Long queryTimeout;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
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

    public String getMasters() {
        return masters;
    }

    public void setMasters(String masters) {
        this.masters = masters;
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

    public boolean isEnableKrb() {
        return enableKrb;
    }

    public void setEnableKrb(boolean enableKrb) {
        this.enableKrb = enableKrb;
    }

    public static KuduCommonConf from(ReadableConfig readableConfig, KuduCommonConf conf) {
        // common
        conf.setMasters(readableConfig.get(MASTER_ADDRESS));
        conf.setTable(readableConfig.get(TABLE_NAME));
        conf.setWorkerCount(readableConfig.get(WORKER_COUNT));
        conf.setParallelism(readableConfig.get(SCAN_PARALLELISM));
        conf.setEnableKrb(readableConfig.get(ENABLE_KRB));

        // timeout
        conf.setQueryTimeout(readableConfig.get(QUERY_TIMEOUT));
        conf.setAdminOperationTimeout(readableConfig.get(ADMIN_OPERATION_TIMEOUT));
        conf.setOperationTimeout(readableConfig.get(OPERATION_TIMEOUT));

        // kerberos
        conf.setAuthentication(readableConfig.get(AUTHENTICATION));
        conf.setPrincipal(readableConfig.get(PRINCIPAL));
        conf.setKeytabFile(readableConfig.get(KEYTAB_FILE));

        return conf;
    }
}
