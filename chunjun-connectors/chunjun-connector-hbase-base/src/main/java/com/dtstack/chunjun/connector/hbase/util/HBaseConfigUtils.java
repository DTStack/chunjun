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

package com.dtstack.chunjun.connector.hbase.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.dtstack.chunjun.security.KerberosUtil.KRB_STR;

/** The utility class of HBase connection */
@Slf4j
public class HBaseConfigUtils {

    private static final String KEY_HBASE_SECURITY_AUTHENTICATION = "hbase.security.authentication";
    private static final String KEY_HBASE_SECURITY_AUTHORIZATION = "hbase.security.authorization";
    private static final String KEY_HBASE_MASTER_KERBEROS_PRINCIPAL =
            "hbase.master.kerberos.principal";
    public static final String KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL =
            "hbase.regionserver.kerberos.principal";

    public static final String KEY_HBASE_CLIENT_KEYTAB_FILE = "hbase.client.keytab.file";
    public static final String KEY_HBASE_CLIENT_KERBEROS_PRINCIPAL =
            "hbase.client.kerberos.principal";

    private static final String KEY_HBASE_SECURITY_AUTH_ENABLE = "hbase.security.auth.enable";
    public static final String KEY_ZOOKEEPER_SASL_CLIENT = "zookeeper.sasl.client";
    public static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    public static final String KEY_KEY_TAB = "hbase.keytab";
    public static final String KEY_PRINCIPAL = "hbase.principal";

    private static final List<String> KEYS_KERBEROS_REQUIRED =
            Arrays.asList(
                    KEY_HBASE_SECURITY_AUTHENTICATION, KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL);

    public static Configuration getConfig(Map<String, Object> hbaseConfigMap) {
        Configuration hConfiguration = HBaseConfiguration.create();

        for (Map.Entry<String, Object> entry : hbaseConfigMap.entrySet()) {
            if (entry.getValue() != null && !(entry.getValue() instanceof Map)) {
                hConfiguration.set(entry.getKey(), entry.getValue().toString());
            }
        }
        return hConfiguration;
    }

    public static boolean isEnableKerberos(Configuration configuration) {
        boolean hasAuthorization =
                KRB_STR.equalsIgnoreCase(configuration.get(KEY_HBASE_SECURITY_AUTHORIZATION));
        boolean hasAuthentication =
                KRB_STR.equalsIgnoreCase(configuration.get(KEY_HBASE_SECURITY_AUTHENTICATION));
        boolean hasAuthEnable =
                Boolean.getBoolean(configuration.get(KEY_HBASE_SECURITY_AUTH_ENABLE));

        if (hasAuthentication || hasAuthorization || hasAuthEnable) {
            log.info("Enable kerberos for hbase.");
            return true;
        }
        return false;
    }

    public static boolean isEnableKerberos(Map<String, Object> hbaseConfigMap) {
        boolean hasAuthorization =
                KRB_STR.equalsIgnoreCase(
                        MapUtils.getString(hbaseConfigMap, KEY_HBASE_SECURITY_AUTHORIZATION));
        boolean hasAuthentication =
                KRB_STR.equalsIgnoreCase(
                        MapUtils.getString(hbaseConfigMap, KEY_HBASE_SECURITY_AUTHENTICATION));
        boolean hasAuthEnable =
                MapUtils.getBooleanValue(hbaseConfigMap, KEY_HBASE_SECURITY_AUTH_ENABLE);

        if (hasAuthentication || hasAuthorization || hasAuthEnable) {
            log.info("Enable kerberos for hbase.");
            setKerberosConf(hbaseConfigMap);
            return true;
        }
        return false;
    }

    public static void setKerberosConf(Map<String, Object> hbaseConfigMap) {
        hbaseConfigMap.put(KEY_HBASE_SECURITY_AUTHORIZATION, KRB_STR);
        hbaseConfigMap.put(KEY_HBASE_SECURITY_AUTHENTICATION, KRB_STR);
        hbaseConfigMap.put(KEY_HBASE_SECURITY_AUTH_ENABLE, true);
    }

    public static Configuration getHadoopConfiguration(Map<String, Object> hbaseConfigMap) {
        for (String key : KEYS_KERBEROS_REQUIRED) {
            if (StringUtils.isEmpty(MapUtils.getString(hbaseConfigMap, key))) {
                throw new IllegalArgumentException(
                        String.format("Must provide [%s] when authentication is Kerberos", key));
            }
        }
        return HBaseConfigUtils.getConfig(hbaseConfigMap);
    }

    public static String getPrincipal(Map<String, Object> hbaseConfigMap) {
        String principal = MapUtils.getString(hbaseConfigMap, KEY_PRINCIPAL);
        if (StringUtils.isNotEmpty(principal)) {
            return principal;
        }

        throw new IllegalArgumentException(KEY_PRINCIPAL + " is not set!");
    }

    public static void fillKerberosConfig(Map<String, Object> hbaseConfigMap) {
        if (StringUtils.isEmpty(
                MapUtils.getString(hbaseConfigMap, KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL))) {
            throw new IllegalArgumentException(
                    "Must provide region server Principal when authentication is Kerberos");
        }

        String regionServerPrincipal =
                MapUtils.getString(hbaseConfigMap, KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL);

        hbaseConfigMap.put(
                HBaseConfigUtils.KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL, regionServerPrincipal);

        hbaseConfigMap.put(HBaseConfigUtils.KEY_HBASE_SECURITY_AUTHORIZATION, "true");
        hbaseConfigMap.put(HBaseConfigUtils.KEY_HBASE_SECURITY_AUTHENTICATION, KRB_STR);
        hbaseConfigMap.put(HBaseConfigUtils.KEY_HBASE_SECURITY_AUTHORIZATION, KRB_STR);

        if (!StringUtils.isEmpty(MapUtils.getString(hbaseConfigMap, KEY_ZOOKEEPER_SASL_CLIENT))) {
            System.setProperty(
                    HBaseConfigUtils.KEY_ZOOKEEPER_SASL_CLIENT,
                    MapUtils.getString(hbaseConfigMap, KEY_ZOOKEEPER_SASL_CLIENT));
        }

        String principal = HBaseConfigUtils.getPrincipal(hbaseConfigMap);

        String keytab =
                HBaseConfigUtils.loadKeyFromConf(hbaseConfigMap, HBaseConfigUtils.KEY_KEY_TAB);
        String krb5Conf =
                HBaseConfigUtils.loadKeyFromConf(
                        hbaseConfigMap, HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF);
        hbaseConfigMap.put(HBaseConfigUtils.KEY_HBASE_CLIENT_KEYTAB_FILE, keytab);
        hbaseConfigMap.put(HBaseConfigUtils.KEY_HBASE_CLIENT_KERBEROS_PRINCIPAL, principal);
        hbaseConfigMap.put(HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF, krb5Conf);
    }

    public static void fillSyncKerberosConfig(
            Configuration config, Map<String, Object> hbaseConfigMap) {
        if (StringUtils.isEmpty(
                MapUtils.getString(hbaseConfigMap, KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL))) {
            throw new IllegalArgumentException(
                    "Must provide region server Principal when authentication is Kerberos");
        }

        String regionServerPrincipal =
                MapUtils.getString(hbaseConfigMap, KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL);
        config.set(HBaseConfigUtils.KEY_HBASE_MASTER_KERBEROS_PRINCIPAL, regionServerPrincipal);
        config.set(
                HBaseConfigUtils.KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL, regionServerPrincipal);
        config.set(HBaseConfigUtils.KEY_HBASE_SECURITY_AUTHORIZATION, "true");
        config.set(HBaseConfigUtils.KEY_HBASE_SECURITY_AUTHENTICATION, KRB_STR);
        config.set(HBaseConfigUtils.KEY_HBASE_SECURITY_AUTHORIZATION, KRB_STR);
    }

    public static String loadKeyFromConf(Map<String, Object> config, String key) {
        String value = MapUtils.getString(config, key);
        if (!StringUtils.isEmpty(value)) {
            String krb5ConfPath;
            if (Paths.get(value).toFile().exists()) {
                krb5ConfPath = value;
            } else {
                krb5ConfPath = System.getProperty("user.dir") + File.separator + value;
            }
            log.info("[{}]:{}", key, krb5ConfPath);
            return krb5ConfPath;
        }
        return value;
    }
}
