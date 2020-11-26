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
package com.dtstack.flinkx.launcher;

import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * KerberosInfo
 *
 * @author by dujie@dtstack.com
 * @Date 2020/8/21
 */
public class KerberosInfo {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosInfo.class);


    private final String krb5confPath;
    private final String keytab;
    private final String principal;
    private final Configuration config;
    private final org.apache.hadoop.conf.Configuration hadoopConfiguration;

    public KerberosInfo(String krb5confPath, String keytab, String principal, Configuration config) {
        this.krb5confPath = krb5confPath;
        this.config = config;
        this.hadoopConfiguration = HadoopUtils.getHadoopConfiguration(this.config);

        //keytab, launcherOptions.getKeytab() 比flinkConfiguration里配置的优先级高
        if (StringUtils.isBlank(keytab)) {
            this.keytab = this.config.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB);
        } else {
            this.keytab = keytab;
        }
        //principal信息, launcherOptions.getPrincipal() 比flinkConfiguration里配置的优先级高
        if (StringUtils.isBlank(principal)) {
            this.principal = this.config.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL);
        } else {
            this.principal = principal;
        }
    }

    public void verify() {
        if (!isVerify()) {
            return;
        }

        check();

        //如果指定了Krb5conf位置
        if (StringUtils.isNotBlank(this.getKrb5confPath())) {
            System.setProperty("java.security.krb5.conf", this.getKrb5confPath());
        }

        String keyTabpath;
        try {
            keyTabpath = (new File(keytab)).getAbsolutePath();
        } catch (Exception e) {
            String message = String.format("can not get the file 【%s】,error info-> %s ",
                    keytab,
                    ExceptionUtil.getErrorMessage(e));
            LOG.error("{}", message);
            throw new RuntimeException(message, e);
        }


        LOG.info("kerberos info:Krb5confPath ->{}, Principal ->{}, keytab->{}", System.getProperty("java.security.krb5.conf"), principal, keyTabpath);

        //开始kerberos验证
        UserGroupInformation.setConfiguration(hadoopConfiguration);
        try {
            UserGroupInformation.getCurrentUser().setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        } catch (IOException e) {
            String message = "UserGroupInformation getCurrentUser has error," + ExceptionUtil.getErrorMessage(e);
            LOG.error("{}", message);
            throw new RuntimeException(message, e);
        }


        try {
            UserGroupInformation.loginUserFromKeytab(principal, keyTabpath);
        } catch (IOException e) {
            String message = String.format("Unable to set the Hadoop login principal【%s】,keytab 【%s】error info-> %s ",
                    principal,
                    keyTabpath,
                    ExceptionUtil.getErrorMessage(e));
            LOG.error("{}", message);
            throw new RuntimeException(message, e);
        }
    }

    //是否需要kerberos验证
    public boolean isVerify() {
        UserGroupInformation.AuthenticationMethod authenticationMethod = SecurityUtil.getAuthenticationMethod(hadoopConfiguration);
        return UserGroupInformation.AuthenticationMethod.SIMPLE != authenticationMethod;
    }

    protected void check() {
        if (StringUtils.isBlank(getKeytab())) {
            throw new RuntimeException("keytabPath can not be null");
        }

        if (StringUtils.isBlank(getPrincipal())) {
            throw new RuntimeException("principal can not be null");
        }
    }


    public String getKrb5confPath() {
        return krb5confPath;
    }

    public String getKeytab() {
        return keytab;
    }


    public String getPrincipal() {
        return principal;
    }


}
