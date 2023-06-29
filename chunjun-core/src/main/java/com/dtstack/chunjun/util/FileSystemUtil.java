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

package com.dtstack.chunjun.util;

import com.dtstack.chunjun.security.KerberosUtil;

import org.apache.flink.api.common.cache.DistributedCache;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import static com.dtstack.chunjun.security.KerberosUtil.KRB_STR;

@Slf4j
public class FileSystemUtil {

    private static final String KEY_HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";
    private static final String KEY_HADOOP_SECURITY_AUTHENTICATION =
            "hadoop.security.authentication";
    private static final String KEY_DEFAULT_FS = "fs.default.name";
    private static final String KEY_FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private static final String KEY_HA_DEFAULT_FS = "fs.defaultFS";
    private static final String KEY_DFS_NAME_SERVICES = "dfs.nameservices";
    private static final String KEY_HADOOP_USER_NAME = "hadoop.user.name";

    public static FileSystem getFileSystem(
            Map<String, Object> hadoopConfigMap,
            String defaultFs,
            DistributedCache distributedCache,
            String jobId,
            String taskNumber)
            throws Exception {
        if (isOpenKerberos(hadoopConfigMap)) {
            return getFsWithKerberos(
                    hadoopConfigMap, defaultFs, distributedCache, jobId, taskNumber);
        }

        Configuration conf = getConfiguration(hadoopConfigMap, defaultFs);
        setHadoopUserName(conf);

        return FileSystem.get(getConfiguration(hadoopConfigMap, defaultFs));
    }

    public static void setHadoopUserName(Configuration conf) {
        String hadoopUserName = conf.get(KEY_HADOOP_USER_NAME);
        if (StringUtils.isEmpty(hadoopUserName)) {
            return;
        }

        try {
            String previousUserName = UserGroupInformation.getLoginUser().getUserName();
            log.info(
                    "Hadoop user from '{}' switch to '{}' with SIMPLE auth",
                    previousUserName,
                    hadoopUserName);
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUserName);
            UserGroupInformation.setLoginUser(ugi);
        } catch (Exception e) {
            log.warn("Set hadoop user name error:", e);
        }
    }

    public static boolean isOpenKerberos(Map<String, Object> hadoopConfig) {
        if (!MapUtils.getBoolean(hadoopConfig, KEY_HADOOP_SECURITY_AUTHORIZATION, false)) {
            return false;
        }

        return KRB_STR.equalsIgnoreCase(
                MapUtils.getString(hadoopConfig, KEY_HADOOP_SECURITY_AUTHENTICATION));
    }

    private static FileSystem getFsWithKerberos(
            Map<String, Object> hadoopConfig,
            String defaultFs,
            DistributedCache distributedCache,
            String jobId,
            String taskNumber)
            throws Exception {
        UserGroupInformation ugi =
                getUGI(hadoopConfig, defaultFs, distributedCache, jobId, taskNumber);

        return ugi.doAs(
                (PrivilegedAction<FileSystem>)
                        () -> {
                            try {
                                return FileSystem.get(getConfiguration(hadoopConfig, defaultFs));
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Get FileSystem with kerberos error:", e);
                            }
                        });
    }

    public static UserGroupInformation getUGI(
            Map<String, Object> hadoopConfig,
            String defaultFs,
            DistributedCache distributedCache,
            String jobId,
            String taskNumber)
            throws IOException {
        String keytabFileName = KerberosUtil.getPrincipalFileName(hadoopConfig);
        keytabFileName =
                KerberosUtil.loadFile(
                        hadoopConfig, keytabFileName, distributedCache, jobId, taskNumber);
        String principal = KerberosUtil.getPrincipal(hadoopConfig, keytabFileName);
        KerberosUtil.loadKrb5Conf(hadoopConfig, distributedCache, jobId, taskNumber);
        KerberosUtil.refreshConfig();

        return KerberosUtil.loginAndReturnUgi(
                getConfiguration(hadoopConfig, defaultFs), principal, keytabFileName);
    }

    public static String getHadoopUser(Map<String, Object> hadoopConfig) throws IOException {
        Object hadoopUserName = hadoopConfig.get("hadoop.user.name");
        if (hadoopUserName != null && StringUtils.isNotBlank(hadoopUserName.toString())) {
            return hadoopUserName.toString();
        }

        hadoopUserName = hadoopConfig.get("HADOOP_USER_NAME");
        if (hadoopUserName != null && StringUtils.isNotBlank(hadoopUserName.toString())) {
            return hadoopUserName.toString();
        }

        return UserGroupInformation.getCurrentUser().getUserName();
    }

    public static Configuration getConfiguration(Map<String, Object> confMap, String defaultFs) {
        confMap = fillConfig(confMap, defaultFs);

        Configuration conf = new Configuration();
        confMap.forEach(
                (key, val) -> {
                    if (val != null) {
                        conf.set(key, val.toString());
                    }
                });

        return conf;
    }

    public static JobConf getJobConf(Map<String, Object> confMap, String defaultFs) {
        confMap = fillConfig(confMap, defaultFs);

        JobConf jobConf = new JobConf();
        confMap.forEach(
                (key, val) -> {
                    if (val != null) {
                        jobConf.set(key, val.toString());
                    }
                });

        return jobConf;
    }

    private static Map<String, Object> fillConfig(Map<String, Object> confMap, String defaultFs) {
        if (confMap == null) {
            confMap = new HashMap<>();
        }

        if (isHaMode(confMap)) {
            if (defaultFs != null) {
                confMap.put(KEY_HA_DEFAULT_FS, defaultFs);
            }
        } else {
            if (defaultFs != null) {
                confMap.put(KEY_DEFAULT_FS, defaultFs);
            }
        }

        confMap.put(KEY_FS_HDFS_IMPL_DISABLE_CACHE, "true");
        return confMap;
    }

    private static boolean isHaMode(Map<String, Object> confMap) {
        return StringUtils.isNotEmpty(MapUtils.getString(confMap, KEY_DFS_NAME_SERVICES));
    }
}
