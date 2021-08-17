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

package com.dtstack.flinkx.connector.hbase14.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The utility class of HBase connection
 *
 * <p>Date: 2019/12/24 Company: www.dtstack.com
 *
 * @author maqi
 */
public class HBaseConfigUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseConfigUtils.class);
    // sync side kerberos
    private static final String AUTHENTICATION_TYPE = "Kerberos";
    private static final String KEY_HBASE_SECURITY_AUTHENTICATION = "hbase.security.authentication";
    private static final String KEY_HBASE_SECURITY_AUTHORIZATION = "hbase.security.authorization";
    private static final String KEY_HBASE_MASTER_KERBEROS_PRINCIPAL =
            "hbase.master.kerberos.principal";
    private static final String KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL =
            "hbase.regionserver.kerberos.principal";

    public static final String KEY_HBASE_CLIENT_KEYTAB_FILE = "hbase.client.keytab.file";
    public static final String KEY_HBASE_CLIENT_KERBEROS_PRINCIPAL =
            "hbase.client.kerberos.principal";

    public static final String KEY_HADOOP_SECURITY_AUTHENTICATION =
            "hadoop.security.authentication";
    public static final String KEY_HADOOP_SECURITY_AUTH_TO_LOCAL = "hadoop.security.auth_to_local";
    public static final String KEY_HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";

    // async side kerberos
    private static final String KEY_HBASE_SECURITY_AUTH_ENABLE = "hbase.security.auth.enable";
    public static final String KEY_HBASE_KERBEROS_REGIONSERVER_PRINCIPAL =
            "hbase.kerberos.regionserver.principal";
    public static final String KEY_KEY_TAB = "hbase.keytab";
    public static final String KEY_PRINCIPAL = "hbase.principal";

    public static final String KEY_HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String KEY_HBASE_ZOOKEEPER_ZNODE_QUORUM = "hbase.zookeeper.znode.parent";

    public static final String KEY_ZOOKEEPER_SASL_CLIENT = "zookeeper.sasl.client";
    private static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    private static List<String> KEYS_KERBEROS_REQUIRED =
            Arrays.asList(
                    KEY_HBASE_SECURITY_AUTHENTICATION,
                    KEY_HBASE_KERBEROS_REGIONSERVER_PRINCIPAL,
                    KEY_PRINCIPAL,
                    KEY_KEY_TAB,
                    KEY_JAVA_SECURITY_KRB5_CONF);

    public static Configuration getConfig(Map<String, Object> hbaseConfigMap) {
        Configuration hConfiguration = HBaseConfiguration.create();

        for (Map.Entry<String, Object> entry : hbaseConfigMap.entrySet()) {
            if (entry.getValue() != null && !(entry.getValue() instanceof Map)) {
                hConfiguration.set(entry.getKey(), entry.getValue().toString());
            }
        }
        return hConfiguration;
    }

    public static boolean isEnableKerberos(Map<String, Object> hbaseConfigMap) {
        boolean hasAuthorization =
                AUTHENTICATION_TYPE.equalsIgnoreCase(
                        MapUtils.getString(hbaseConfigMap, KEY_HBASE_SECURITY_AUTHORIZATION));
        boolean hasAuthentication =
                AUTHENTICATION_TYPE.equalsIgnoreCase(
                        MapUtils.getString(hbaseConfigMap, KEY_HBASE_SECURITY_AUTHENTICATION));
        boolean hasAuthEnable =
                MapUtils.getBooleanValue(hbaseConfigMap, KEY_HBASE_SECURITY_AUTH_ENABLE);

        if (hasAuthentication || hasAuthorization || hasAuthEnable) {
            LOG.info("Enable kerberos for hbase.");
            setKerberosConf(hbaseConfigMap);
            return true;
        }
        return false;
    }

    private static void setKerberosConf(Map<String, Object> hbaseConfigMap) {
        hbaseConfigMap.put(KEY_HBASE_SECURITY_AUTHORIZATION, AUTHENTICATION_TYPE);
        hbaseConfigMap.put(KEY_HBASE_SECURITY_AUTHENTICATION, AUTHENTICATION_TYPE);
        hbaseConfigMap.put(KEY_HBASE_SECURITY_AUTH_ENABLE, true);
    }

    public static Configuration getHadoopConfiguration(Map<String, Object> hbaseConfigMap) {
        for (String key : KEYS_KERBEROS_REQUIRED) {
            if (StringUtils.isEmpty(MapUtils.getString(hbaseConfigMap, key))) {
                throw new IllegalArgumentException(
                        String.format("Must provide [%s] when authentication is Kerberos", key));
            }
        }
        return HBaseConfiguration.create();
    }

    public static String getPrincipal(Map<String, Object> hbaseConfigMap) {
        String principal = MapUtils.getString(hbaseConfigMap, KEY_PRINCIPAL);
        if (StringUtils.isNotEmpty(principal)) {
            return principal;
        }

        throw new IllegalArgumentException("");
    }

    public static String getKeytab(Map<String, Object> hbaseConfigMap) {
        String keytab = MapUtils.getString(hbaseConfigMap, KEY_KEY_TAB);
        if (StringUtils.isNotEmpty(keytab)) {
            return keytab;
        }

        throw new IllegalArgumentException("");
    }

    public static void fillSyncKerberosConfig(
            Configuration config, Map<String, Object> hbaseConfigMap) throws IOException {
        if (StringUtils.isEmpty(
                MapUtils.getString(hbaseConfigMap, KEY_HBASE_KERBEROS_REGIONSERVER_PRINCIPAL))) {
            throw new IllegalArgumentException(
                    "Must provide regionserverPrincipal when authentication is Kerberos");
        }

        String regionserverPrincipal =
                MapUtils.getString(hbaseConfigMap, KEY_HBASE_KERBEROS_REGIONSERVER_PRINCIPAL);
        config.set(HBaseConfigUtils.KEY_HBASE_MASTER_KERBEROS_PRINCIPAL, regionserverPrincipal);
        config.set(
                HBaseConfigUtils.KEY_HBASE_REGIONSERVER_KERBEROS_PRINCIPAL, regionserverPrincipal);
        config.set(HBaseConfigUtils.KEY_HBASE_SECURITY_AUTHORIZATION, "true");
        config.set(HBaseConfigUtils.KEY_HBASE_SECURITY_AUTHENTICATION, "kerberos");

        if (!StringUtils.isEmpty(MapUtils.getString(hbaseConfigMap, KEY_ZOOKEEPER_SASL_CLIENT))) {
            System.setProperty(
                    HBaseConfigUtils.KEY_ZOOKEEPER_SASL_CLIENT,
                    MapUtils.getString(hbaseConfigMap, KEY_ZOOKEEPER_SASL_CLIENT));
        }

        String securityKrb5Conf = MapUtils.getString(hbaseConfigMap, KEY_JAVA_SECURITY_KRB5_CONF);
        if (!StringUtils.isEmpty(securityKrb5Conf)) {
            String krb5ConfPath =
                    System.getProperty("user.dir") + File.separator + securityKrb5Conf;
            LOG.info("krb5ConfPath:{}", krb5ConfPath);
            System.setProperty(HBaseConfigUtils.KEY_JAVA_SECURITY_KRB5_CONF, krb5ConfPath);
        }
    }

    public static void loadKrb5Conf(Map<String, Object> config) {
        String krb5conf = MapUtils.getString(config, KEY_JAVA_SECURITY_KRB5_CONF);
        checkOpt(krb5conf, KEY_JAVA_SECURITY_KRB5_CONF);
        String krb5FilePath =
                System.getProperty("user.dir")
                        + File.separator
                        + MapUtils.getString(config, KEY_JAVA_SECURITY_KRB5_CONF);
        DtFileUtils.checkExists(krb5FilePath);
        System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);
        LOG.info("{} is set to {}", KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);
    }

    // TODO 日后改造可以下沉到Core模块
    public static void checkOpt(String opt, String key) {
        Preconditions.checkState(!Strings.isNullOrEmpty(opt), "%s must be set!", key);
    }

    public static UserGroupInformation loginAndReturnUGI(
            Configuration conf, String principal, String keytab) throws IOException {
        if (conf == null) {
            throw new IllegalArgumentException("kerberos conf can not be null");
        }

        if (org.apache.commons.lang.StringUtils.isEmpty(principal)) {
            throw new IllegalArgumentException("principal can not be null");
        }

        if (org.apache.commons.lang.StringUtils.isEmpty(keytab)) {
            throw new IllegalArgumentException("keytab can not be null");
        }

        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);

        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
    }

    public static UserGroupInformation loginAndReturnUGI2(
            Configuration conf, String principal, String keytab) throws IOException, KrbException {
        LOG.info("loginAndReturnUGI principal {}", principal);
        LOG.info("loginAndReturnUGI keytab {}", keytab);
        if (conf == null) {
            throw new IllegalArgumentException("kerberos conf can not be null");
        }

        if (org.apache.commons.lang.StringUtils.isEmpty(principal)) {
            throw new IllegalArgumentException("principal can not be null");
        }

        if (org.apache.commons.lang.StringUtils.isEmpty(keytab)) {
            throw new IllegalArgumentException("keytab can not be null");
        }

        if (!new File(keytab).exists()) {
            throw new IllegalArgumentIOException("keytab [" + keytab + "] not exist");
        }

        conf.set(KEY_HADOOP_SECURITY_AUTHENTICATION, "Kerberos");
        // conf.set("hadoop.security.auth_to_local", "DEFAULT");
        conf.set(KEY_HADOOP_SECURITY_AUTH_TO_LOCAL, "RULE:[1:$1] RULE:[2:$1]");
        conf.set(KEY_HADOOP_SECURITY_AUTHORIZATION, "true");

        Config.refresh();
        KerberosName.resetDefaultRealm();
        UserGroupInformation.setConfiguration(conf);

        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
    }
}
