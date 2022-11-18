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

package com.dtstack.chunjun.connector.hbase.util;

import com.dtstack.chunjun.connector.hbase.config.HBaseConfig;
import com.dtstack.chunjun.security.KerberosUtil;
import com.dtstack.chunjun.util.FileSystemUtil;

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.dtstack.chunjun.connector.hbase.util.HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF;
import static com.dtstack.chunjun.security.KerberosUtil.KRB_STR;

/** The utility class of HBase */
public class HBaseHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseHelper.class);

    private static final String KEY_HBASE_SECURITY_AUTHENTICATION = "hbase.security.authentication";
    private static final String KEY_HBASE_SECURITY_AUTHORIZATION = "hbase.security.authorization";
    private static final String KEY_HBASE_SECURITY_AUTH_ENABLE = "hbase.security.auth.enable";

    public static Connection getHbaseConnection(HBaseConfig hBaseConfig) {
        Map<String, Object> hbaseConfig = new HashMap<>(hBaseConfig.getHbaseConfig());
        return getHbaseConnection(hbaseConfig);
    }

    public static Connection getHbaseConnection(Map<String, Object> hbaseConfigMap) {
        Validate.isTrue(MapUtils.isNotEmpty(hbaseConfigMap), "hbaseConfig不能为空Map结构!");

        if (HBaseConfigUtils.isEnableKerberos(hbaseConfigMap)) {
            return getConnectionWithKerberos(hbaseConfigMap);
        }

        try {
            Configuration hConfiguration = getConfig(hbaseConfigMap);
            return ConnectionFactory.createConnection(hConfiguration);
        } catch (IOException e) {
            LOG.error("Get connection fail with config:{}", hbaseConfigMap);
            throw new RuntimeException(e);
        }
    }

    private static Connection getConnectionWithKerberos(Map<String, Object> hbaseConfigMap) {
        try {
            setKerberosConf(hbaseConfigMap);
            UserGroupInformation ugi = getUgi(hbaseConfigMap);
            return ugi.doAs(
                    (PrivilegedAction<Connection>)
                            () -> {
                                try {
                                    Configuration hConfiguration = getConfig(hbaseConfigMap);
                                    return ConnectionFactory.createConnection(hConfiguration);
                                } catch (IOException e) {
                                    LOG.error("Get connection fail with config:{}", hbaseConfigMap);
                                    throw new RuntimeException(e);
                                }
                            });
        } catch (Exception e) {
            throw new RuntimeException("Login kerberos error", e);
        }
    }

    public static UserGroupInformation getUgi(Map<String, Object> hbaseConfigMap)
            throws IOException {
        String keytabFileName = KerberosUtil.getPrincipalFileName(hbaseConfigMap);

        keytabFileName = KerberosUtil.loadFile(hbaseConfigMap, keytabFileName);
        String principal = KerberosUtil.getPrincipal(hbaseConfigMap, keytabFileName);
        KerberosUtil.loadKrb5Conf(hbaseConfigMap);
        KerberosUtil.refreshConfig();

        Configuration conf = FileSystemUtil.getConfiguration(hbaseConfigMap, null);
        return KerberosUtil.loginAndReturnUgi(
                principal, keytabFileName, System.getProperty(KEY_JAVA_SECURITY_KRB5_CONF));
    }

    public static Configuration getConfig(Map<String, Object> hbaseConfigMap) {
        Configuration hConfiguration = HBaseConfiguration.create();
        if (MapUtils.isEmpty(hbaseConfigMap)) {
            return hConfiguration;
        }

        for (Map.Entry<String, Object> entry : hbaseConfigMap.entrySet()) {
            if (entry.getValue() != null && !(entry.getValue() instanceof Map)) {
                hConfiguration.set(entry.getKey(), entry.getValue().toString());
            }
        }

        return hConfiguration;
    }

    /** 设置hbase 开启kerberos 连接必要的固定参数 */
    public static void setKerberosConf(Map<String, Object> hbaseConfigMap) {
        hbaseConfigMap.put(KEY_HBASE_SECURITY_AUTHORIZATION, KRB_STR);
        hbaseConfigMap.put(KEY_HBASE_SECURITY_AUTHENTICATION, KRB_STR);
        hbaseConfigMap.put(KEY_HBASE_SECURITY_AUTH_ENABLE, true);
    }

    public static RegionLocator getRegionLocator(Connection hConnection, String userTable) {
        TableName hTableName = TableName.valueOf(userTable);
        Admin admin = null;
        RegionLocator regionLocator = null;
        try {
            admin = hConnection.getAdmin();
            HBaseHelper.checkHbaseTable(admin, hTableName);
            regionLocator = hConnection.getRegionLocator(hTableName);
        } catch (Exception e) {
            HBaseHelper.closeRegionLocator(regionLocator);
            HBaseHelper.closeAdmin(admin);
            HBaseHelper.closeConnection(hConnection);
            throw new RuntimeException(e);
        }
        return regionLocator;
    }

    public static byte[] convertRowKey(String rowKey, boolean isBinaryRowkey) {
        if (StringUtils.isBlank(rowKey)) {
            return HConstants.EMPTY_BYTE_ARRAY;
        } else {
            return HBaseHelper.stringToBytes(rowKey, isBinaryRowkey);
        }
    }

    private static byte[] stringToBytes(String rowKey, boolean isBinaryRowKey) {
        if (isBinaryRowKey) {
            return Bytes.toBytesBinary(rowKey);
        } else {
            return Bytes.toBytes(rowKey);
        }
    }

    public static void closeConnection(Connection hConnection) {
        try {
            if (null != hConnection) {
                hConnection.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeAdmin(Admin admin) {
        try {
            if (null != admin) {
                admin.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeRegionLocator(RegionLocator regionLocator) {
        try {
            if (null != regionLocator) {
                regionLocator.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void checkHbaseTable(Admin admin, TableName table) throws IOException {
        if (!admin.tableExists(table)) {
            throw new IllegalArgumentException("hbase table " + table + " does not exist.");
        }
        if (!admin.isTableAvailable(table)) {
            throw new RuntimeException("hbase table " + table + " is not available.");
        }
        if (admin.isTableDisabled(table)) {
            throw new RuntimeException("hbase table " + table + " is disabled");
        }
    }

    public static void closeBufferedMutator(BufferedMutator bufferedMutator) {
        try {
            if (null != bufferedMutator) {
                bufferedMutator.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void scheduleRefreshTGT(UserGroupInformation ugi) {
        final ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("UserGroupInformation-Relogin"));

        executor.scheduleWithFixedDelay(
                () -> {
                    try {
                        ugi.checkTGTAndReloginFromKeytab();
                    } catch (Exception e) {
                        LOG.error("Refresh TGT failed", e);
                    }
                },
                0,
                1,
                TimeUnit.HOURS);
    }
}
