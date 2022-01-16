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

package com.dtstack.flinkx.connector.hbase14.table.lookup;

import com.dtstack.flinkx.connector.hbase.HBaseConfigurationUtil;
import com.dtstack.flinkx.connector.hbase.HBaseSerde;
import com.dtstack.flinkx.connector.hbase.HBaseTableSchema;
import com.dtstack.flinkx.connector.hbase14.util.HBaseConfigUtils;
import com.dtstack.flinkx.lookup.AbstractAllTableFunction;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.security.KerberosUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.time.LocalDateTime;
import java.util.Map;

public class HBaseAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseAllTableFunction.class);
    private Configuration conf;
    private final byte[] serializedConfig;
    private Connection conn;
    private String tableName;
    private Table table;
    private ResultScanner resultScanner;

    private final HBaseTableSchema hbaseTableSchema;
    private transient HBaseSerde serde;
    private final String nullStringLiteral;

    public HBaseAllTableFunction(
            Configuration conf,
            LookupConf lookupConf,
            HBaseTableSchema hbaseTableSchema,
            String nullStringLiteral) {
        super(null, null, lookupConf, null);
        this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(conf);
        this.tableName = hbaseTableSchema.getTableName();
        this.hbaseTableSchema = hbaseTableSchema;
        this.nullStringLiteral = nullStringLiteral;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        this.serde = new HBaseSerde(hbaseTableSchema, nullStringLiteral);
        super.open(context);
    }

    @Override
    protected void loadData(Object cacheRef) {
        conf = HBaseConfigurationUtil.prepareRuntimeConfiguration(serializedConfig);
        int loadDataCount = 0;
        try {
            if (HBaseConfigUtils.isEnableKerberos(conf)) {
                String principal = conf.get(HBaseConfigUtils.KEY_HBASE_CLIENT_KERBEROS_PRINCIPAL);
                String keytab = conf.get(HBaseConfigUtils.KEY_HBASE_CLIENT_KEYTAB_FILE);
                String krb5Conf = conf.get(HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF);
                LOG.info("kerberos principal:{}，keytab:{}", principal, keytab);
                System.setProperty(HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF, krb5Conf);
                UserGroupInformation userGroupInformation =
                        KerberosUtil.loginAndReturnUgi(principal, keytab, krb5Conf);
                Configuration finalConf = conf;
                conn =
                        userGroupInformation.doAs(
                                (PrivilegedAction<Connection>)
                                        () -> {
                                            try {
                                                ScheduledChore authChore =
                                                        AuthUtil.getAuthChore(finalConf);
                                                if (authChore != null) {
                                                    ChoreService choreService =
                                                            new ChoreService("hbaseKerberosSink");
                                                    choreService.scheduleChore(authChore);
                                                }

                                                return ConnectionFactory.createConnection(
                                                        finalConf);

                                            } catch (IOException e) {
                                                LOG.error(
                                                        "Get connection fail with config:{}",
                                                        finalConf);
                                                throw new RuntimeException(e);
                                            }
                                        });

            } else {
                conn = ConnectionFactory.createConnection(conf);
            }

            table = conn.getTable(TableName.valueOf(tableName));
            resultScanner = table.getScanner(new Scan());
            Map<Object, RowData> tmpCache = (Map<Object, RowData>) cacheRef;
            for (Result r : resultScanner) {
                tmpCache.put(serde.getRowKey(r.getRow()), serde.convertToReusedRow(r));
                loadDataCount++;
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            LOG.info("load Data count: {}", loadDataCount);
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
                LOG.error("", e);
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
        LOG.info(
                "----- " + lookupConf.getTableName() + ": all cacheRef reload end:{}",
                LocalDateTime.now());
    }

    /**
     * The invoke entry point of lookup function.
     *
     * @param keys the lookup key. Currently only support single rowkey.
     */
    @Override
    public void eval(Object... keys) {
        Map<Object, RowData> cache = (Map<Object, RowData>) cacheRef.get();
        RowData rowData = cache.get(keys[0]);
        if (rowData == null) {
            return;
        }
        collect(rowData);
    }
}
