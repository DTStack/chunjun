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

import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.google.common.base.Strings;

/**
 * Kerberos of certain connectors could be enabled, it should be implements this interface.
 * e.g. HBaseConf class in HBase connector.
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/15
 */
public interface KerberosConfig {

    String getPrincipal();

    void setPrincipal(String principal);

    String getKeytab();

    void setKeytab(String keytab);

    String getKrb5conf();

    void setKrb5conf(String krb5conf);

    boolean isEnableKrb();

    void setEnableKrb(boolean enableKrb);

    default void judgeKrbEnable() {
        boolean allSet =
                !Strings.isNullOrEmpty(getPrincipal())
                        && !Strings.isNullOrEmpty(getKeytab())
                        && !Strings.isNullOrEmpty(getKrb5conf());

        boolean allNotSet =
                Strings.isNullOrEmpty(getPrincipal())
                        && Strings.isNullOrEmpty(getKeytab())
                        && Strings.isNullOrEmpty(getKrb5conf());

        if (allSet) {
            setEnableKrb(true);
        } else if (allNotSet) {
            setEnableKrb(false);
        } else {
            throw new FlinkxRuntimeException(
                    "Missing kerberos parameter! all kerberos params must be set, or all kerberos params are not set");
        }
    }
}
