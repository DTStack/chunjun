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

package com.dtstack.chunjun.connector.kudu.config;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.security.KerberosConfig;

import org.apache.flink.configuration.ReadableConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections.MapUtils;

import java.util.HashMap;
import java.util.Map;

import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.ADMIN_OPERATION_TIMEOUT;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.MASTER_ADDRESS;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.OPERATION_TIMEOUT;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.QUERY_TIMEOUT;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.TABLE_NAME;
import static com.dtstack.chunjun.connector.kudu.table.KuduOptions.WORKER_COUNT;
import static com.dtstack.chunjun.security.KerberosOptions.KEYTAB;
import static com.dtstack.chunjun.security.KerberosOptions.KRB5_CONF;
import static com.dtstack.chunjun.security.KerberosOptions.PRINCIPAL;
import static com.dtstack.chunjun.source.options.SourceOptions.SCAN_PARALLELISM;

@EqualsAndHashCode(callSuper = true)
@Data
public class KuduCommonConfig extends CommonConfig {

    private static final long serialVersionUID = 7636567670954773869L;

    /** master节点地址:端口，多个以,隔开 */
    protected String masters;

    /** kudu表名 */
    protected String table;

    /** kudu kerberos */
    protected KerberosConfig kerberos;

    /** hadoop高可用相关配置 * */
    private Map<String, Object> hadoopConfig = new HashMap<>(16);

    /** worker线程数，默认为cpu*2 */
    protected Integer workerCount = 2;

    /** 设置普通操作超时时间，默认30S */
    protected Long operationTimeout = 30 * 1000L;

    /** 设置管理员操作(建表，删表)超时时间，默认30S */
    protected Long adminOperationTimeout = 30 * 1000L;

    /** 连接scan token的超时时间，如果不设置，则与operationTimeout一致 */
    protected Long queryTimeout = 30 * 1000L;

    public KerberosConfig getKerberos() {
        kerberos.judgeAndSetKrbEnabled();
        return kerberos;
    }

    public static KuduCommonConfig from(ReadableConfig readableConfig, KuduCommonConfig conf) {
        // common
        conf.setMasters(readableConfig.get(MASTER_ADDRESS));
        conf.setTable(readableConfig.get(TABLE_NAME));
        conf.setWorkerCount(readableConfig.get(WORKER_COUNT));
        conf.setParallelism(readableConfig.get(SCAN_PARALLELISM));

        // timeout
        conf.setQueryTimeout(readableConfig.get(QUERY_TIMEOUT));
        conf.setAdminOperationTimeout(readableConfig.get(ADMIN_OPERATION_TIMEOUT));
        conf.setOperationTimeout(readableConfig.get(OPERATION_TIMEOUT));

        // kerberos
        String principal = readableConfig.get(PRINCIPAL);
        String keytab = readableConfig.get(KEYTAB);
        String krb5Conf = readableConfig.get(KRB5_CONF);

        KerberosConfig kerberosConfig = new KerberosConfig(principal, keytab, krb5Conf);
        conf.setKerberos(kerberosConfig);

        return conf;
    }

    public KerberosConfig conventHadoopConfig() {

        String principal = MapUtils.getString(hadoopConfig, "principal");
        String keytab = MapUtils.getString(hadoopConfig, "principalFile");
        String krb5Conf = MapUtils.getString(hadoopConfig, "java.security.krb5.conf");

        return new KerberosConfig(principal, keytab, krb5Conf);
    }
}
