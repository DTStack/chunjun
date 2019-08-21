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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * @author jiangbo
 * @date 2019/8/20
 */
public class KerberosUtil {

    public static Logger LOG = LoggerFactory.getLogger(KerberosUtil.class);

    private static final String HOST_PLACEHOLDER = "_HOST";
    private static final String PRINCIPAL_SPLIT_REGEX = "[/@]";
    private static final String SFTP_FILE_SCHEMA = "sftp";

    private static final String KEYWORD_PRINCIPAL = "principal";
    private static final String KEYWORD_KEYTAB = "keytab";

    private static final String KEY_IDENTIFY = "identify";
    private static final String KEY_LOCAL_PATH = "localPath";

    private static final String DEFAULT_LOCAL_PATH = "/tmp/dtstack/keytab";

    public static void loadKeyTabFilesAndReplaceHost(Map<String, String> kerberosConfig) {
        if(kerberosConfig == null || kerberosConfig.isEmpty()){
            throw new IllegalArgumentException("The kerberos config is null");
        }

        String localPath = MapUtils.getString(kerberosConfig, KEY_LOCAL_PATH, DEFAULT_LOCAL_PATH)
                + File.separator
                + MapUtils.getString(kerberosConfig, KEY_IDENTIFY, UUID.randomUUID().toString());
        boolean isFirstLoad = checkAndCreateLocalPath(localPath);

        SFTPHandler handler = null;
        try {
            handler = SFTPHandler.getInstance(kerberosConfig);

            for (String key : kerberosConfig.keySet()) {
                if (key.contains(KEYWORD_PRINCIPAL)) {
                    kerberosConfig.put(key, replaceHost(kerberosConfig.get(key)));
                } else if(key.contains(KEYWORD_KEYTAB)){
                    String keytabPath = loadKeyTabFile(kerberosConfig.get(key), localPath, handler, isFirstLoad);
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

    private static boolean checkAndCreateLocalPath(String localPath){
        File file = new File(localPath);
        if (file.exists()){
            if (file.isFile()){
                throw new RuntimeException("local path is a file,not a directory:" + localPath);
            }

            return false;
        } else {
            boolean result = file.mkdir();
            if (!result){
                throw new RuntimeException();
            }

            return true;
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
        String fqdn = InetAddress.getLocalHost().getCanonicalHostName();
        return components[0] + "/" + fqdn.toLowerCase(Locale.US) + "@" + components[2];
    }

    private static String loadKeyTabFile(String remoteFile, String localPath, SFTPHandler handler, boolean isFirstLoad){
        if(remoteFile.contains(HOST_PLACEHOLDER)){
            try {
                String fqdn = InetAddress.getLocalHost().getCanonicalHostName();
                remoteFile = remoteFile.replace(HOST_PLACEHOLDER, fqdn);
            } catch (IOException e){
                throw new RuntimeException("Replace host placeholder error:" + remoteFile, e);
            }
        }

        URI uri;
        try {
            uri = new URI(remoteFile);
        } catch (URISyntaxException e){
            throw new RuntimeException("", e);
        }

        // 没有指定存储方式，则从本地路径查找
        if(uri.getScheme() == null){
            if(isFileExistLocal(uri.getPath())){
                return uri.getPath();
            } else {
                throw new RuntimeException("keytab file not exists:" + uri.getPath());
            }
        }

        if(!uri.getScheme().equals(SFTP_FILE_SCHEMA)){
            throw new RuntimeException("Only support download from sftp");
        }

        String fileName = getFileName(remoteFile);
        String localFile = localPath + File.separator + fileName;
        if(!isFirstLoad && isFileExistLocal(localFile)){
            return localFile;
        }

        handler.downloadFile(uri.getPath(), localFile);
        return localFile;
    }

    private static String getFileName(String remoteFile){
        String[] splits = remoteFile.split("/");
        return splits[splits.length - 1];
    }

    private static boolean isFileExistLocal(String localFile){
        File file = new File(localFile);
        return file.exists();
    }

    public static void main(String[] args) {
        Map<String, String> kerberosConfig = new HashMap<>();
        kerberosConfig.put("principal", "hbase/_HOST@DTSTACK>COM");
        kerberosConfig.put("keytab", "sftp:///root/_HOST.hbase.keytab");
        kerberosConfig.put("host", "172.16.10.69");
        kerberosConfig.put("port", "22");
        kerberosConfig.put("username", "root");
        kerberosConfig.put("password", "abc123");
        kerberosConfig.put("localPath", "D:\\");
        kerberosConfig.put("identify", "localtest_dwdas");

        loadKeyTabFilesAndReplaceHost(kerberosConfig);
    }
}
