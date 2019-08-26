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

import java.io.IOException;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/21
 */
public class FileSystemUtil {

    private static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    private static final String AUTHENTICATION_TYPE = "Kerberos";
    private static final String KEY_HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";
    private static final String KEY_HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";

    public static FileSystem getFileSystem(Map<String, Object> hadoopConfig, String defaultFS, String jobId, String plugin) throws Exception {
        if(openKerberos(hadoopConfig)){
            login(hadoopConfig, jobId, plugin);
        }

        Configuration conf = getHadoopConfig(hadoopConfig, defaultFS);
        return FileSystem.get(conf);
    }

    private static boolean openKerberos(Map<String, Object> hadoopConfig){
        if(!MapUtils.getBoolean(hadoopConfig, KEY_HADOOP_SECURITY_AUTHORIZATION)){
            return false;
        }

        return AUTHENTICATION_TYPE.equalsIgnoreCase(MapUtils.getString(hadoopConfig, KEY_HADOOP_SECURITY_AUTHENTICATION));
    }

    private static void login(Map<String, Object> hadoopConfig, String jobId, String plugin) throws IOException {
        KerberosUtil.loadKeyTabFilesAndReplaceHost(hadoopConfig, jobId, plugin);

        Configuration conf = getHadoopConfig(hadoopConfig, null);
        if(StringUtils.isNotEmpty(conf.get(KEY_JAVA_SECURITY_KRB5_CONF))){
            System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, conf.get(KEY_JAVA_SECURITY_KRB5_CONF));
        }

        UserGroupInformation.setConfiguration(conf);

        // TODO get principal and keytab
        String principal = "";
        String keytab = "";
        UserGroupInformation.loginUserFromKeytab(principal, keytab);
    }

    public static Configuration getHadoopConfig(Map<String, Object> confMap, String defaultFS) {
        Configuration conf = new Configuration();

        if (confMap != null) {
            for (Map.Entry<String, Object> entry : confMap.entrySet()) {
                if(entry.getValue() != null && !(entry.getValue() instanceof Map)){
                    conf.set(entry.getKey(), entry.getValue().toString());
                }
            }
        }

        if(defaultFS != null){
            conf.set("fs.default.name", defaultFS);
        } else {
            defaultFS = MapUtils.getString(confMap, "fs.defaultFS");
            if(StringUtils.isNotEmpty(defaultFS)){
                conf.set("fs.default.name", defaultFS);
            }
        }

        conf.set("fs.hdfs.impl.disable.cache", "true");

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
            conf.set("fs.default.name", defaultFS);
        }

        conf.set("fs.hdfs.impl.disable.cache", "true");

        return conf;
    }
}
