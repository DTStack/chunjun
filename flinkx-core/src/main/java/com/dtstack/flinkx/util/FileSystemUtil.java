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

import java.security.PrivilegedAction;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/21
 */
public class FileSystemUtil {

    private static final String AUTHENTICATION_TYPE = "Kerberos";
    private static final String KEY_HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";
    private static final String KEY_HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    private static final String KEY_DFS_NAMENODE_KERBEROS_RINCIPAL = "dfs.namenode.kerberos.principal";
    private static final String KEY_DFS_NAMENODE_KEYTAB_FILE = "dfs.namenode.keytab.file";
    private static final String KEY_FS_DEFAULT_NAME = "fs.default.name";
    private static final String KEY_FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private static final String KEY_FS_DEFAULTFS = "fs.defaultFS";

    public static FileSystem getFileSystem(Map<String, Object> hadoopConfig, String defaultFS, String jobId, String plugin) throws Exception {
        if(openKerberos(hadoopConfig)){
            return getFsWithKerberos(hadoopConfig, jobId, plugin, defaultFS);
        }

        return FileSystem.get(getConfiguration(hadoopConfig, defaultFS));
    }

    private static boolean openKerberos(Map<String, Object> hadoopConfig){
        if(!MapUtils.getBoolean(hadoopConfig, KEY_HADOOP_SECURITY_AUTHORIZATION, false)){
            return false;
        }

        return AUTHENTICATION_TYPE.equalsIgnoreCase(MapUtils.getString(hadoopConfig, KEY_HADOOP_SECURITY_AUTHENTICATION));
    }

    private static FileSystem getFsWithKerberos(Map<String, Object> hadoopConfig, String jobId, String plugin, String defaultFS) throws Exception{
        String keytab = getKeytab(hadoopConfig);
        String principal = getPrincipal(hadoopConfig);

        keytab = KerberosUtil.loadFile(hadoopConfig, keytab, jobId, plugin);
        principal = KerberosUtil.findPrincipalFromKeytab(principal, keytab);
        KerberosUtil.loadKrb5Conf(hadoopConfig, jobId, plugin);

        UserGroupInformation ugi = KerberosUtil.loginAndReturnUGI(getConfiguration(hadoopConfig, defaultFS), principal, keytab);
        return ugi.doAs(new PrivilegedAction<FileSystem>() {
            @Override
            public FileSystem run(){
                try {
                    return FileSystem.get(getConfiguration(hadoopConfig, defaultFS));
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

    public static Configuration getConfiguration(Map<String, Object> confMap, String defaultFS) {
        Configuration conf = new Configuration();

        if (confMap != null) {
            for (Map.Entry<String, Object> entry : confMap.entrySet()) {
                if(entry.getValue() != null && !(entry.getValue() instanceof Map)){
                    conf.set(entry.getKey(), entry.getValue().toString());
                }
            }
        }

        if(defaultFS != null){
            conf.set(KEY_FS_DEFAULT_NAME, defaultFS);
        } else {
            defaultFS = MapUtils.getString(confMap, KEY_FS_DEFAULTFS);
            if(StringUtils.isNotEmpty(defaultFS)){
                conf.set(KEY_FS_DEFAULT_NAME, defaultFS);
            }
        }

        conf.set(KEY_FS_HDFS_IMPL_DISABLE_CACHE, "true");

        return conf;
    }

    public static JobConf getJobConf(Map<String, Object> confMap, String defaultFS){
        JobConf conf = new JobConf();

        if (confMap != null) {
            for (Map.Entry<String, Object> entry : confMap.entrySet()) {
                if(entry.getValue() != null && !(entry.getValue() instanceof Map)){
                    conf.set(entry.getKey(), entry.getValue().toString());
                }
            }
        }

        if(defaultFS != null){
            conf.set(KEY_FS_DEFAULT_NAME, defaultFS);
        }

        conf.set(KEY_FS_HDFS_IMPL_DISABLE_CACHE, "true");

        return conf;
    }
}
