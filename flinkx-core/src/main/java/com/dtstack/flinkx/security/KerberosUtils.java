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

package com.dtstack.flinkx.security;

import org.apache.flink.runtime.security.DynamicConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;

import javax.security.auth.login.AppConfigurationEntry;

import java.io.IOException;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/11
 */
public class KerberosUtils {

    public static final String KRB5_CONF_KEY = "java.security.krb5.conf";
    public static final String HADOOP_AUTH_KEY = "hadoop.security.authentication";
    public static final String KRB_STR = "Kerberos";

    private static final Logger LOG = LoggerFactory.getLogger(KerberosUtils.class);
    //    public static final String FALSE_STR = "false";
    //    public static final String SUBJECT_ONLY_KEY = "javax.security.auth.useSubjectCredsOnly";

    public static UserGroupInformation loginAndReturnUgi(
            String principal, String keytabPath, String krb5confPath) throws IOException {
        LOG.info("Kerberos login with principal: {} and keytab: {}", principal, keytabPath);
        reloadKrb5conf(krb5confPath);
        // TODO 尚未探索出此选项的意义，以后研究明白方可打开
        //        System.setProperty(SUBJECT_ONLY_KEY, FALSE_STR);
        Configuration configuration = new Configuration();
        configuration.set(HADOOP_AUTH_KEY, KRB_STR);
        UserGroupInformation.setConfiguration(configuration);
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
    }

    public static UserGroupInformation loginAndReturnUgi(KerberosConfig kerberosConfig)
            throws IOException {
        String principal = kerberosConfig.getPrincipal();
        String keytabPath = kerberosConfig.getKeytab();
        String krb5confPath = kerberosConfig.getKrb5conf();
        LOG.info("Kerberos login with principal: {} and keytab: {}", principal, keytabPath);
        reloadKrb5conf(krb5confPath);
        // TODO 尚未探索出此选项的意义，以后研究明白方可打开
        //        System.setProperty(SUBJECT_ONLY_KEY, FALSE_STR);
        Configuration configuration = new Configuration();
        configuration.set(HADOOP_AUTH_KEY, KRB_STR);
        UserGroupInformation.setConfiguration(configuration);
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
    }

    public static synchronized void appendJaasConf(String name, String keytab, String principal) {
        javax.security.auth.login.Configuration priorConfig =
                javax.security.auth.login.Configuration.getConfiguration();
        // construct a dynamic JAAS configuration
        DynamicConfiguration currentConfig = new DynamicConfiguration(priorConfig);
        // wire up the configured JAAS login contexts to use the krb5 entries
        AppConfigurationEntry krb5Entry =
                org.apache.flink.runtime.security.KerberosUtils.keytabEntry(keytab, principal);
        currentConfig.addAppConfigurationEntry(name, krb5Entry);
        javax.security.auth.login.Configuration.setConfiguration(currentConfig);
    }

    public static synchronized void reloadKrb5conf(String krb5confPath) {
        System.setProperty(KRB5_CONF_KEY, krb5confPath);
        // 不刷新会读/etc/krb5.conf
        try {
            Config.refresh();
            KerberosName.resetDefaultRealm();
        } catch (KrbException e) {
            LOG.warn(
                    "resetting default realm failed, current default realm will still be used.", e);
        }
    }
}
