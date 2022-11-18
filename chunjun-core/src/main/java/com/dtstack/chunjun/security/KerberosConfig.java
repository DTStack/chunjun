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

package com.dtstack.chunjun.security;

import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import com.google.common.base.Strings;

import java.io.Serializable;
import java.util.Map;

/**
 * Kerberos of certain connectors could be enabled, it should use or extends this class. e.g.
 * KuduInputFormat class can combine it.
 */
public class KerberosConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    protected boolean enableKrb;
    protected String principal;
    protected String keytab;
    protected String krb5conf;
    protected String remoteDir;
    Map<String, String> sftpConf;

    // do not delete. Preserving empty parameter construction method for JsonUtil.toObject()
    public KerberosConfig() {}

    public KerberosConfig(String principal, String keytab, String krb5conf) {
        this.principal = principal;
        this.keytab = keytab;
        this.krb5conf = krb5conf;
        judgeAndSetKrbEnabled();
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getKeytab() {
        return keytab;
    }

    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

    public String getKrb5conf() {
        return krb5conf;
    }

    public void setKrb5conf(String krb5conf) {
        this.krb5conf = krb5conf;
    }

    public String getRemoteDir() {
        return remoteDir;
    }

    public void setRemoteDir(String remoteDir) {
        this.remoteDir = remoteDir;
    }

    public Map<String, String> getSftpConf() {
        return sftpConf;
    }

    public void setSftpConf(Map<String, String> sftpConf) {
        this.sftpConf = sftpConf;
    }

    public boolean isEnableKrb() {
        return enableKrb;
    }

    public void judgeAndSetKrbEnabled() {
        boolean allSet =
                !Strings.isNullOrEmpty(getPrincipal())
                        && !Strings.isNullOrEmpty(getKeytab())
                        && !Strings.isNullOrEmpty(getKrb5conf());

        boolean allNotSet =
                Strings.isNullOrEmpty(getPrincipal())
                        && Strings.isNullOrEmpty(getKeytab())
                        && Strings.isNullOrEmpty(getKrb5conf());

        if (allSet) {
            this.enableKrb = true;
        } else if (allNotSet) {
            this.enableKrb = false;
        } else {
            throw new ChunJunRuntimeException(
                    "Missing kerberos parameter! all kerberos params must be set, or all kerberos params are not set");
        }
    }
}
