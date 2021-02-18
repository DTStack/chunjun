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

package com.dtstack.flinkx.metastore.constants;

import com.dtstack.flinkx.authenticate.KerberosUtil;
import com.dtstack.flinkx.metastore.entity.MetaStoreClientInfo;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.FileSystemUtil;
import com.dtstack.flinkx.util.RetryUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;

import static com.dtstack.flinkx.metatdata.hive2.core.util.Hive2MetaDataCons.KEY_PRINCIPAL;


/**
 * @author toutian
 */

public final class MetaStoreClientUtil {


    private static Logger LOG = LoggerFactory.getLogger(MetaStoreClientUtil.class);

    public static HiveMetaStoreClient getClient(MetaStoreClientInfo metaStoreClientInfo) {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set(MetaDataCons.META_STORE, metaStoreClientInfo.getMetaStoreUrl());
        String principal = MapUtils.getString(metaStoreClientInfo.getHiveConf(), KEY_PRINCIPAL);
        if (StringUtils.isNotEmpty(principal)) {
            return getClinetWithKerberos(metaStoreClientInfo, hiveConf);
        }
        return getClientWithRetry(metaStoreClientInfo, hiveConf);
    }

    private static HiveMetaStoreClient getClientWithRetry(MetaStoreClientInfo metaStoreClientInfo, HiveConf hiveConf) {
        try {
            return RetryUtil.executeWithRetry(() -> connect(hiveConf), 1, 1000L, false);
        } catch (Exception e1) {
            throw new RuntimeException(String.format("连接：%s 时发生错误：%s.", metaStoreClientInfo.getMetaStoreUrl(), ExceptionUtil.getErrorMessage(e1)), e1);
        }
    }

    public static HiveMetaStoreClient connect(HiveConf hiveConf) throws MetaException {
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        return hiveMetaStoreClient;

    }

    public static HiveMetaStoreClient getClinetWithKerberos(MetaStoreClientInfo metaStoreClientInfo, HiveConf hiveConf) {
        String keytabFileName = KerberosUtil.getPrincipalFileName(metaStoreClientInfo.getHiveConf());
        keytabFileName = KerberosUtil.loadFile(metaStoreClientInfo.getHiveConf(), keytabFileName);
        KerberosUtil.loadKrb5Conf(metaStoreClientInfo.getHiveConf());
        String principal = KerberosUtil.getPrincipal(metaStoreClientInfo.getHiveConf(), keytabFileName);
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, principal);
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keytabFileName);
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, "true");
        Configuration conf = FileSystemUtil.getConfiguration(metaStoreClientInfo.getHiveConf(), null);
        UserGroupInformation ugi;
        try {
            ugi = KerberosUtil.loginAndReturnUgi(conf, principal, keytabFileName);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException("Login kerberos error:", e);
        }
        LOG.info("current ugi:{}", ugi);
        return ugi.doAs((PrivilegedAction<HiveMetaStoreClient>) () -> getClientWithRetry(metaStoreClientInfo, hiveConf));

    }
}
