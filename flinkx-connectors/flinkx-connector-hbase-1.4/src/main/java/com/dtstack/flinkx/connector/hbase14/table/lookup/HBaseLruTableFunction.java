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

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hbase.HBaseTableSchema;
import com.dtstack.flinkx.connector.hbase14.conf.HBaseConf;
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
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.hbase.async.Config;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

import javax.security.auth.login.AppConfigurationEntry;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HBaseLruTableFunction extends AbstractLruTableFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseLruTableFunction.class);
    private final HBaseConf conf;
    private static final int DEFAULT_BOSS_THREADS = 1;
    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int DEFAULT_POOL_SIZE = DEFAULT_IO_THREADS + DEFAULT_BOSS_THREADS;
    private transient HBaseClient hBaseClient;
    private String tableName;
    private String[] colNames;

    private final HBaseTableSchema hbaseTableSchema;
    private transient AsyncHBaseSerde serde;

    public HBaseLruTableFunction(
            HBaseConf conf, LookupConf lookupConf, HBaseTableSchema hbaseTableSchema) {
        super(lookupConf, null);
        this.conf = conf;
        this.lookupConf = lookupConf;
        this.hbaseTableSchema = hbaseTableSchema;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        this.serde = new AsyncHBaseSerde(hbaseTableSchema, conf.getNullMode());
        tableName = conf.getTableName();
        colNames =
                conf.getColumnMetaInfos().stream().map(FieldConf::getName).toArray(String[]::new);
        Map<String, Object> hbaseConfig = conf.getHbaseConfig();
        ExecutorService executorService =
                new ThreadPoolExecutor(
                        DEFAULT_POOL_SIZE,
                        DEFAULT_POOL_SIZE,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new FlinkxThreadFactory("hbase-async"));

        Config config = new Config();
        config.overrideConfig(
                HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM,
                (String) conf.getHbaseConfig().get(HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_QUORUM));
        config.overrideConfig(
                HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM,
                (String)
                        conf.getHbaseConfig()
                                .get(HBaseConfigUtils.KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM));
        hbaseConfig.forEach((key, value) -> config.overrideConfig(key, (String) value));

        if (HBaseConfigUtils.isEnableKerberos(hbaseConfig)) {
            HBaseConfigUtils.loadKrb5Conf(hbaseConfig);
            String principal = MapUtils.getString(hbaseConfig, HBaseConfigUtils.KEY_PRINCIPAL);
            HBaseConfigUtils.checkOpt(principal, HBaseConfigUtils.KEY_PRINCIPAL);
            String regionserverPrincipal =
                    MapUtils.getString(
                            hbaseConfig,
                            HBaseConfigUtils.KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL);
            HBaseConfigUtils.checkOpt(
                    regionserverPrincipal,
                    HBaseConfigUtils.KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL);
            String keytab = MapUtils.getString(hbaseConfig, HBaseConfigUtils.KEY_KEY_TAB);
            HBaseConfigUtils.checkOpt(keytab, HBaseConfigUtils.KEY_KEY_TAB);
            String keytabPath = System.getProperty("user.dir") + File.separator + keytab;
            DtFileUtils.checkExists(keytabPath);

            LOG.info("Kerberos login with keytab: {} and principal: {}", keytab, principal);
            String name = "HBaseClient";
            config.overrideConfig("hbase.sasl.clientconfig", name);
            appendJaasConf(name, keytab, principal);
            refreshConfig();
        }

        hBaseClient = new HBaseClient(config, executorService);
        try {
            Deferred deferred =
                    hBaseClient.ensureTableExists(tableName).addCallbacks(arg -> arg, arg -> arg);

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
                                Map<String, byte[]> sideMap = Maps.newHashMap();
                                for (KeyValue keyValue : keyValues) {
                                    String cf = new String(keyValue.family());
                                    String col = new String(keyValue.qualifier());
                                    String mapKey = cf + ":" + col;
                                    sideMap.put(mapKey, keyValue.value());
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

    protected RowData fillData(Object sideInput) throws Exception {
        return rowConverter.toInternalLookup(sideInput);
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
