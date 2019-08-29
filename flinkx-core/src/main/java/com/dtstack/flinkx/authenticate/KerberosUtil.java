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


package com.dtstack.flinkx.authenticate;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.internal.ktab.KeyTab;
import sun.security.krb5.internal.ktab.KeyTabEntry;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Locale;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/20
 */
public class KerberosUtil {

    public static Logger LOG = LoggerFactory.getLogger(KerberosUtil.class);

    private static final String PRINCIPAL_SPLIT_REGEX = "/";
    private static final String SP = File.separator;

    private static final String KEY_SFTP_CONF = "sftpConf";
    private static final String KEY_REMOTE_DIR = "remoteDir";
    private static final String KEY_USE_LOCAL_FILE = "useLocalFile";
    private static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    private static String LOCAL_DIR;

    static {
        String systemInfo = System.getProperty("os.name");
        if(systemInfo.toLowerCase().startsWith("windows")){
            LOCAL_DIR = System.getProperty("user.dir");
        } else {
            LOCAL_DIR = "/tmp/flinkx/keytab";
        }
    }

    public static void login(Configuration conf, String principal, String keytab) throws IOException {
        if (conf == null) {
            throw new IllegalArgumentException("kerberos conf can not be null");
        }

        if (StringUtils.isEmpty(principal)) {
            throw new IllegalArgumentException("principal can not be null");
        }

        if(StringUtils.isEmpty(keytab)){
            throw new IllegalArgumentException("keytab can not be null");
        }

        if(StringUtils.isNotEmpty(conf.get(KEY_JAVA_SECURITY_KRB5_CONF))){
            System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, conf.get(KEY_JAVA_SECURITY_KRB5_CONF));
        }

        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);

        UserGroupInformation.loginUserFromKeytab(principal, keytab);
    }

    public static void loadKrb5Conf(Map<String, Object> kerberosConfig, String jobId, String plugin){
        String krb5FilePath = MapUtils.getString(kerberosConfig, KEY_JAVA_SECURITY_KRB5_CONF);
        if(StringUtils.isEmpty(krb5FilePath)){
            return;
        }

        krb5FilePath = loadFile(kerberosConfig, krb5FilePath, jobId, plugin);
        kerberosConfig.put(KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);
    }

    public static String loadFile(Map<String, Object> kerberosConfig, String filePath, String jobId, String plugin) {
        boolean useLocalFile = MapUtils.getBooleanValue(kerberosConfig, KEY_USE_LOCAL_FILE);
        if(useLocalFile){
            checkFileExists(filePath);
        } else {
            if(filePath.contains(SP)){
                filePath = filePath.substring(filePath.lastIndexOf(SP) + 1);
            }

            filePath = loadFromSFTP(kerberosConfig, filePath, jobId, plugin);
        }

        return filePath;
    }

    private static void checkFileExists(String keytab){
       File file = new File(keytab);
       if (file.exists()){
           if (file.isDirectory()) {
               throw new RuntimeException("keytab is a directory:" + keytab);
           }
       } else {
           throw new RuntimeException("keytab file not exists:" + keytab);
       }
    }

    private static String loadFromSFTP(Map<String, Object> config, String keytab, String jobId, String plugin){
        String localDir = createLocalDir(jobId, plugin);
        String localPath = localDir + SP + keytab;

        SFTPHandler handler = null;
        try {
            handler = SFTPHandler.getInstance(MapUtils.getMap(config, KEY_SFTP_CONF));

            String remoteDir = MapUtils.getString(config, KEY_REMOTE_DIR);
            String filePathOnSFTP = remoteDir + "/" + keytab;
            if(handler.isFileExist(filePathOnSFTP)){
                handler.downloadFile(filePathOnSFTP, localPath);
                return localPath;
            } else {
                String hostname = InetAddress.getLocalHost().getCanonicalHostName().toLowerCase(Locale.US);
                filePathOnSFTP = remoteDir + "/" + hostname + "/" + keytab;
                handler.downloadFile(filePathOnSFTP, localPath);
                return localPath;
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            if (handler != null){
                handler.close();
            }
        }
    }

    public static String findPrincipalFromKeytab(String principal, String keytab) {
        String serverName = principal.split(PRINCIPAL_SPLIT_REGEX)[0];

        KeyTab keyTab = KeyTab.getInstance(keytab);
        for (KeyTabEntry entry : keyTab.getEntries()) {
            String princ = entry.getService().getName();
            if(princ.startsWith(serverName)){
                return princ;
            }
        }

        return principal;
    }

    public static void clear(String jobId){
        File file = new File(LOCAL_DIR + SP + jobId);
        if (file.exists()){
            boolean result = file.delete();
            if (!result){
                LOG.warn("Delete file failure:[{}]", LOCAL_DIR + SP + jobId);
            }
        }
    }

    private static String createLocalDir(String jobId, String plugin){
        String path = LOCAL_DIR + SP + jobId + SP + plugin;
        File file = new File(path);
        if (file.exists()){
            boolean result = file.delete();
            if (!result) {
                throw new RuntimeException("Delete file failure:" + LOCAL_DIR + SP + jobId);
            }
        }

        boolean result = file.mkdirs();
        if (!result){
            throw new RuntimeException();
        }

        return path;
    }
}
