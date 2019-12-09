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


package com.dtstack.flinkx.util;

import com.dtstack.flinkx.authenticate.KerberosUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivilegedAction;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/21
 */
public class FileSystemUtil {

    public static final Logger LOG = LoggerFactory.getLogger(FileSystemUtil.class);

    private static final String AUTHENTICATION_TYPE = "Kerberos";
    private static final String KEY_HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";
    private static final String KEY_HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    private static final String KEY_DFS_NAMENODE_KERBEROS_RINCIPAL = "dfs.namenode.kerberos.principal";
    private static final String KEY_DFS_NAMENODE_KEYTAB_FILE = "dfs.namenode.keytab.file";
    private static final String KEY_DEFAULT_FS = "fs.default.name";
    private static final String KEY_FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private static final String KEY_HA_DEFAULT_FS = "fs.defaultFS";
    private static final String KEY_DFS_NAMESERVICES = "dfs.nameservices";
    private static final String KEY_HADOOP_USER_NAME = "hadoop.user.name";

    public static FileSystem getFileSystem(Map<String, Object> hadoopConfigMap, String defaultFs, String jobId, String plugin) throws Exception {
        if(isOpenKerberos(hadoopConfigMap)){
            return getFsWithKerberos(hadoopConfigMap, jobId, plugin, defaultFs);
        }

        Configuration conf = getConfiguration(hadoopConfigMap, defaultFs);
        setHadoopUserName(conf);

        return FileSystem.get(getConfiguration(hadoopConfigMap, defaultFs));
    }

    public static void setHadoopUserName(Configuration conf){
        String hadoopUserName = conf.get(KEY_HADOOP_USER_NAME);
        if(StringUtils.isEmpty(hadoopUserName)){
            return;
        }

        try {
            String ticketCachePath = conf.get("hadoop.security.kerberos.ticket.cache.path");
            UserGroupInformation ugi = UserGroupInformation.getBestUGI(ticketCachePath, hadoopUserName);
            UserGroupInformation.setLoginUser(ugi);
        } catch (Exception e) {
            LOG.warn("Set hadoop user name error:", e);
        }
    }

    private static boolean isOpenKerberos(Map<String, Object> hadoopConfig){
        if(!MapUtils.getBoolean(hadoopConfig, KEY_HADOOP_SECURITY_AUTHORIZATION, false)){
            return false;
        }

        return AUTHENTICATION_TYPE.equalsIgnoreCase(MapUtils.getString(hadoopConfig, KEY_HADOOP_SECURITY_AUTHENTICATION));
    }

    private static FileSystem getFsWithKerberos(Map<String, Object> hadoopConfig, String jobId, String plugin, String defaultFs) throws Exception{
        String keytab = getKeytab(hadoopConfig);
        String principal = getPrincipal(hadoopConfig);

        keytab = KerberosUtil.loadFile(hadoopConfig, keytab, jobId, plugin);
        principal = KerberosUtil.findPrincipalFromKeytab(principal, keytab);
        KerberosUtil.loadKrb5Conf(hadoopConfig, jobId, plugin);

        UserGroupInformation ugi = KerberosUtil.loginAndReturnUGI(getConfiguration(hadoopConfig, defaultFs), principal, keytab);
        return ugi.doAs(new PrivilegedAction<FileSystem>() {
            @Override
            public FileSystem run(){
                try {
                    return FileSystem.get(getConfiguration(hadoopConfig, defaultFs));
                } catch (Exception e){
                    throw new RuntimeException("Get FileSystem with kerberos error:", e);
                }
            }
        });
    }

    private static String getPrincipal(Map<String, Object> hadoopConfig){
        String principal = MapUtils.getString(hadoopConfig, KEY_DFS_NAMENODE_KERBEROS_RINCIPAL);
        if(StringUtils.isNotEmpty(principal)){
            return principal;
        }

        throw new IllegalArgumentException("Can not find principal from hadoopConfig");
    }

    private static String getKeytab(Map<String, Object> hadoopConfig){
        String keytab = MapUtils.getString(hadoopConfig, KEY_DFS_NAMENODE_KEYTAB_FILE);
        if(StringUtils.isNotEmpty(keytab)){
            return keytab;
        }

        throw new IllegalArgumentException("Can not find keytab from hadoopConfig");
    }

    public static Configuration getConfiguration(Map<String, Object> confMap, String defaultFs) {
        fillConfig(confMap, defaultFs);

        Configuration conf = new Configuration();
        if(confMap == null){
            return conf;
        }

        confMap.forEach((key, val) -> {
            if(val != null){
                conf.set(key, val.toString());
            }
        });

        return conf;
    }

    public static JobConf getJobConf(Map<String, Object> confMap, String defaultFs){
        fillConfig(confMap, defaultFs);

        JobConf jobConf = new JobConf();
        if (confMap == null) {
            return jobConf;
        }

        confMap.forEach((key, val) -> {
            if(val != null){
                jobConf.set(key, val.toString());
            }
        });

        return jobConf;
    }

    private static void fillConfig(Map<String, Object> confMap, String defaultFs) {
        if (confMap == null) {
            return;
        }

        if (isHaMode(confMap)) {
            if(defaultFs != null){
                confMap.put(KEY_HA_DEFAULT_FS, defaultFs);
            }
        } else {
            if(defaultFs != null){
                confMap.put(KEY_DEFAULT_FS, defaultFs);
            }
        }

        confMap.put(KEY_FS_HDFS_IMPL_DISABLE_CACHE, "true");
    }

    private static boolean isHaMode(Map<String, Object> confMap){
        return StringUtils.isNotEmpty(MapUtils.getString(confMap, KEY_DFS_NAMESERVICES));
    }
}
