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
import com.dtstack.flinkx.connector.hbase.HBaseTableSchema;
import com.dtstack.flinkx.connector.hbase14.converter.AsyncHBaseSerde;
import com.dtstack.flinkx.connector.hbase14.util.DtFileUtils;
import com.dtstack.flinkx.connector.hbase14.util.HBaseConfigUtils;
import com.dtstack.flinkx.enums.ECacheContentType;
import com.dtstack.flinkx.factory.FlinkxThreadFactory;
import com.dtstack.flinkx.lookup.AbstractLruTableFunction;
import com.dtstack.flinkx.lookup.cache.CacheMissVal;
import com.dtstack.flinkx.lookup.cache.CacheObj;
import com.dtstack.flinkx.lookup.conf.LookupConf;

import org.apache.flink.runtime.security.DynamicConfiguration;
import org.apache.flink.runtime.security.KerberosUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.hbase.async.Config;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

import javax.security.auth.login.AppConfigurationEntry;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HBaseLruTableFunction extends AbstractLruTableFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseLruTableFunction.class);
    private Config asyncClientConfig;
    private Configuration conf;
    private final byte[] serializedConfig;
    private final String nullStringLiteral;
    private static final int DEFAULT_BOSS_THREADS = 1;
    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int DEFAULT_POOL_SIZE = DEFAULT_IO_THREADS + DEFAULT_BOSS_THREADS;
    private transient HBaseClient hBaseClient;
    private String tableName;

    private final HBaseTableSchema hbaseTableSchema;
    private transient AsyncHBaseSerde serde;

    public HBaseLruTableFunction(
            Configuration conf,
            LookupConf lookupConf,
            HBaseTableSchema hbaseTableSchema,
            String nullStringLiteral) {
        super(lookupConf, null);
        this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(conf);
        this.lookupConf = lookupConf;
        this.hbaseTableSchema = hbaseTableSchema;
        this.nullStringLiteral = nullStringLiteral;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        conf = HBaseConfigurationUtil.prepareRuntimeConfiguration(serializedConfig);
        asyncClientConfig = new Config();
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            asyncClientConfig.overrideConfig(entry.getKey(), entry.getValue());
        }
        this.serde = new AsyncHBaseSerde(hbaseTableSchema, nullStringLiteral);
        tableName = hbaseTableSchema.getTableName();
        ExecutorService executorService =
                new ThreadPoolExecutor(
                        DEFAULT_POOL_SIZE,
                        DEFAULT_POOL_SIZE,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new FlinkxThreadFactory("hbase-async"));
        if (HBaseConfigUtils.isEnableKerberos(conf)) {
            System.setProperty(
                    HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF,
                    asyncClientConfig.getString(HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF));
            String principal =
                    asyncClientConfig.getString(
                            HBaseConfigUtils.KEY_HBASE_CLIENT_KERBEROS_PRINCIPAL);
            String keytab =
                    asyncClientConfig.getString(HBaseConfigUtils.KEY_HBASE_CLIENT_KEYTAB_FILE);
            DtFileUtils.checkExists(keytab);
            LOG.info("Kerberos login with keytab: {} and principal: {}", keytab, principal);
            String name = "HBaseClient";
            asyncClientConfig.overrideConfig("hbase.sasl.clientconfig", name);
            appendJaasConf(name, keytab, principal);
            refreshConfig();
        }

        hBaseClient = new HBaseClient(asyncClientConfig, executorService);
        try {
            Deferred deferred =
                    hBaseClient
                            .ensureTableExists(tableName)
                            .addCallbacks(
                                    arg -> new CheckResult(true, ""),
                                    arg -> new CheckResult(false, arg.toString()));

            CheckResult result = (CheckResult) deferred.join();
            if (!result.isConnect()) {
                throw new RuntimeException(result.getExceptionMsg());
            }

        } catch (Exception e) {
            throw new RuntimeException("create hbase connection fail:", e);
        }
    }

    @Override
    public void handleAsyncInvoke(
            CompletableFuture<Collection<RowData>> future, Object... rowKeys) {
        Object rowKey = rowKeys[0];
        byte[] key = serde.getRowKey(rowKey);
        String keyStr = new String(key);
        GetRequest getRequest = new GetRequest(tableName, key);
        hBaseClient
                .get(getRequest)
                .addCallbacks(
                        keyValues -> {
                            try {
                                Map<String, Map<String, byte[]>> sideMap = Maps.newHashMap();
                                for (KeyValue keyValue : keyValues) {
                                    String cf = new String(keyValue.family());
                                    String col = new String(keyValue.qualifier());
                                    if (!sideMap.containsKey(cf)) {
                                        Map<String, byte[]> cfMap = Maps.newHashMap();
                                        cfMap.put(col, keyValue.value());
                                        sideMap.put(cf, cfMap);
                                    } else {
                                        sideMap.get(cf).putIfAbsent(col, keyValue.value());
                                    }
                                }
                                RowData rowData = serde.convertToNewRow(sideMap, key);
                                if (keyValues.size() > 0) {
                                    try {
                                        if (openCache()) {
                                            sideCache.putCache(
                                                    keyStr,
                                                    CacheObj.buildCacheObj(
                                                            ECacheContentType.MultiLine,
                                                            Collections.singletonList(rowData)));
                                        }
                                        future.complete(Collections.singletonList(rowData));
                                    } catch (Exception e) {
                                        future.completeExceptionally(e);
                                    }
                                } else {
                                    dealMissKey(future);
                                    if (openCache()) {
                                        sideCache.putCache(keyStr, CacheMissVal.getMissKeyObj());
                                    }
                                }
                            } catch (Exception e) {
                                future.completeExceptionally(e);
                                LOG.error("record:" + keyStr);
                                LOG.error("get side record exception:", e);
                            }
                            return "";
                        },
                        o -> {
                            LOG.error("record:" + keyStr);
                            LOG.error("get side record exception:" + o);
                            future.complete(Collections.EMPTY_LIST);
                            return "";
                        });
    }

    private void refreshConfig() throws KrbException {
        sun.security.krb5.Config.refresh();
        KerberosName.resetDefaultRealm();
        // reload java.security.auth.login.config
        // javax.security.auth.login.Configuration.setConfiguration(null);
    }

    private void appendJaasConf(String name, String keytab, String principal) {
        javax.security.auth.login.Configuration priorConfig =
                javax.security.auth.login.Configuration.getConfiguration();
        // construct a dynamic JAAS configuration
        DynamicConfiguration currentConfig = new DynamicConfiguration(priorConfig);
        // wire up the configured JAAS login contexts to use the krb5 entries
        AppConfigurationEntry krb5Entry = KerberosUtils.keytabEntry(keytab, principal);
        currentConfig.addAppConfigurationEntry(name, krb5Entry);
        javax.security.auth.login.Configuration.setConfiguration(currentConfig);
    }

    @Override
    public void close() throws Exception {
        super.close();
        hBaseClient.shutdown();
    }

    class CheckResult {
        private boolean connect;

        private String exceptionMsg;

        CheckResult(boolean connect, String msg) {
            this.connect = connect;
            this.exceptionMsg = msg;
        }

        public boolean isConnect() {
            return connect;
        }

        public void setConnect(boolean connect) {
            this.connect = connect;
        }

        public String getExceptionMsg() {
            return exceptionMsg;
        }

        public void setExceptionMsg(String exceptionMsg) {
            this.exceptionMsg = exceptionMsg;
        }
    }
}
