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

package com.dtstack.chunjun.connector.hbase.table.lookup;

import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.connector.hbase.converter.HBaseSerde;
import com.dtstack.chunjun.connector.hbase.util.HBaseConfigUtils;
import com.dtstack.chunjun.connector.hbase.util.HBaseHelper;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.security.KerberosUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.time.LocalDateTime;
import java.util.Map;

@Slf4j
public class HBaseAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = -4528928897188353663L;
    private Connection conn;
    private Table table;
    private ResultScanner resultScanner;

    private final HBaseTableSchema hbaseTableSchema;
    private transient HBaseSerde serde;
    private final HBaseConfig hBaseConfig;

    public HBaseAllTableFunction(
            LookupConfig lookupConfig, HBaseTableSchema hbaseTableSchema, HBaseConfig hBaseConfig) {
        super(null, null, lookupConfig, null);
        this.hbaseTableSchema = hbaseTableSchema;
        this.hBaseConfig = hBaseConfig;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.serde = new HBaseSerde(hbaseTableSchema, hBaseConfig);
        super.open(context);
    }

    @Override
    protected void loadData(Object cacheRef) {
        Configuration hbaseDomainConf = new Configuration();
        for (Map.Entry<String, Object> entry : hBaseConfig.getHbaseConfig().entrySet()) {
            hbaseDomainConf.set(entry.getKey(), entry.getValue().toString());
        }
        int loadDataCount = 0;
        try {
            if (HBaseConfigUtils.isEnableKerberos(hbaseDomainConf)) {
                String principal =
                        hbaseDomainConf.get(HBaseConfigUtils.KEY_HBASE_CLIENT_KERBEROS_PRINCIPAL);
                String keytab = hbaseDomainConf.get(HBaseConfigUtils.KEY_HBASE_CLIENT_KEYTAB_FILE);
                String krb5Conf = hbaseDomainConf.get(HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF);
                log.info("kerberos principal:{}，keytab:{}", principal, keytab);
                System.setProperty(HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF, krb5Conf);
                UserGroupInformation userGroupInformation =
                        KerberosUtil.loginAndReturnUgi(principal, keytab, krb5Conf);
                conn =
                        userGroupInformation.doAs(
                                (PrivilegedAction<Connection>)
                                        () -> {
                                            try {
                                                return ConnectionFactory.createConnection(
                                                        hbaseDomainConf);
                                            } catch (IOException e) {
                                                log.error(
                                                        "Get connection fail with config:{}",
                                                        hbaseDomainConf);
                                                throw new RuntimeException(e);
                                            }
                                        });
                HBaseHelper.scheduleRefreshTGT(userGroupInformation);
            } else {
                conn = ConnectionFactory.createConnection(hbaseDomainConf);
            }
            table = conn.getTable(TableName.valueOf(hbaseTableSchema.getTableName()));
            resultScanner = table.getScanner(new Scan());
            Map<Object, RowData> tmpCache = (Map<Object, RowData>) cacheRef;
            for (Result r : resultScanner) {
                tmpCache.put(serde.getRowKey(r.getRow()), serde.convertToNewRow(r));
                loadDataCount++;
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            log.info("load Data count: {}", loadDataCount);
            try {
                if (null != conn) {
                    conn.close();
                }

                if (null != table) {
                    table.close();
                }

                if (null != resultScanner) {
                    resultScanner.close();
                }
            } catch (IOException e) {
                log.error("", e);
            }
        }
    }

    @Override
    protected void initCache() {
        Map<Object, RowData> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        loadData(newCache);
    }

    /** 定时加载数据库中数据 */
    @Override
    protected void reloadCache() {
        // reload cacheRef and replace to old cacheRef
        Map<Object, RowData> newCache = Maps.newConcurrentMap();
        loadData(newCache);
        cacheRef.set(newCache);
        log.info(
                "----- " + lookupConfig.getTableName() + ": all cacheRef reload end:{}",
                LocalDateTime.now());
    }
}
