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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/20
 */
public class KerberosUtil {

    public static Logger LOG = LoggerFactory.getLogger(KerberosUtil.class);

    private static final String HOST_PLACEHOLDER = "_HOST";
    private static final String PRINCIPAL_SPLIT_REGEX = "[/@]";

    private static final String KEYWORD_PRINCIPAL = "principal";
    private static final String KEYWORD_KEYTAB = "keytab";

    private static final String KEY_SFTP_CONF = "sftpConf";
    private static final String KEY_REMOTE_DIR = "remoteDir";
    private static final String KEY_USE_LOCAL_FILE = "useLocalFile";

    private static final String LOCAL_DIR = "/tmp/dtstack/flinkx/keytab";

    public static void loadKeyTabFilesAndReplaceHost(Map<String, Object> kerberosConfig, String jobId) {
        if(kerberosConfig == null || kerberosConfig.isEmpty()){
            throw new IllegalArgumentException("The kerberos config is null");
        }

        createLocalDir(jobId);

        SFTPHandler handler = null;
        try {
            handler = SFTPHandler.getInstance(MapUtils.getMap(kerberosConfig, KEY_SFTP_CONF));

            boolean useLocalFile = MapUtils.getBooleanValue(kerberosConfig, KEY_USE_LOCAL_FILE);
            String remoteDir = MapUtils.getString(kerberosConfig, KEY_REMOTE_DIR);
            String hostname = InetAddress.getLocalHost().getCanonicalHostName();

            for (String key : kerberosConfig.keySet()) {
                if (key.contains(KEYWORD_PRINCIPAL)) {
                    kerberosConfig.put(key, replaceHost(MapUtils.getString(kerberosConfig, key)));
                } else if(key.contains(KEYWORD_KEYTAB)){
                    String keytabPath;
                    if(useLocalFile){
                        keytabPath = loadFromLocal(MapUtils.getString(kerberosConfig, key));
                    } else {
                        keytabPath = loadFromSftp(MapUtils.getString(kerberosConfig, key), remoteDir, hostname, jobId, handler);
                    }

                    kerberosConfig.put(key, keytabPath);
                }
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            if (handler != null){
                handler.close();
            }
        }
    }

    public static void clear(String jobId){
        File file = new File(LOCAL_DIR + File.separator + jobId);
        if (file.exists()){
            boolean result = file.delete();
            if (!result){
                LOG.warn("Delete file failure:[{}]", LOCAL_DIR + File.separator + jobId);
            }
        }
    }

    private static void createLocalDir(String jobId){
        File file = new File(LOCAL_DIR + File.separator + jobId);
        if (file.exists()){
            boolean result = file.delete();
            if (!result) {
                throw new RuntimeException("Delete file failure:" + LOCAL_DIR + File.separator + jobId);
            }
        }

        boolean result = file.mkdirs();
        if (!result){
            throw new RuntimeException();
        }
    }

    private static String replaceHost(String principalConfig) throws IOException {
        if(StringUtils.isEmpty(principalConfig)){
            return principalConfig;
        }

        String[] components = principalConfig.split(PRINCIPAL_SPLIT_REGEX);
        return components.length == 3 && components[1].equals(HOST_PLACEHOLDER) ? replacePattern(components) : principalConfig;
    }

    private static String replacePattern(String[] components) throws IOException {
        String hostname = InetAddress.getLocalHost().getCanonicalHostName();
        return components[0] + "/" + hostname.toLowerCase(Locale.US) + "@" + components[2];
    }

    private static String loadFromLocal(String localFilePath) throws Exception{
        if(localFilePath.contains(HOST_PLACEHOLDER)){
            String hostname = InetAddress.getLocalHost().getCanonicalHostName();
            localFilePath = localFilePath.replace(HOST_PLACEHOLDER, hostname);
        }

        if(!isFileExistLocal(localFilePath)){
            throw new RuntimeException("File not exists:" + localFilePath);
        }

        return localFilePath;
    }

    private static String loadFromSftp(String fileName, String remoteDir, String hostname, String jobId, SFTPHandler handler){
        String remoteFilePath = remoteDir + "/"  + hostname.toLowerCase(Locale.US) + "/" + fileName;
        String localFile = LOCAL_DIR + File.separator + jobId + File.separator + fileName;
        handler.downloadFile(remoteFilePath, localFile);
        return localFile;
    }

    private static boolean isFileExistLocal(String localFile){
        File file = new File(localFile);
        return file.exists();
    }

    public static void main(String[] args) {
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put("principal", "hbase/_HOST@DTSTACK.COM");
        kerberosConfig.put("keytab", "D://hbase.keytab");
        kerberosConfig.put("remoteDir", "/root");
        kerberosConfig.put("useLocalFile", "true");

        Map<String, String> sftpConf = new HashMap<>();
        sftpConf.put("host", "172.16.10.69");
        sftpConf.put("port", "22");
        sftpConf.put("username", "root");
        sftpConf.put("password", "abc123");

        kerberosConfig.put("sftpConf", sftpConf);

        loadKeyTabFilesAndReplaceHost(kerberosConfig, "dhueawhuxnsjahu");
    }
}
