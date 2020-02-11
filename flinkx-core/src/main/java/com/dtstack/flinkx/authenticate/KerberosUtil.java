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
import sun.security.krb5.Config;
import sun.security.krb5.internal.ktab.KeyTab;
import sun.security.krb5.internal.ktab.KeyTabEntry;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/20
 */
public class KerberosUtil {

    public static Logger LOG = LoggerFactory.getLogger(KerberosUtil.class);

    private static final String SP = "/";

    private static final String KEY_SFTP_CONF = "sftpConf";
    private static final String KEY_REMOTE_DIR = "remoteDir";
    private static final String KEY_USE_LOCAL_FILE = "useLocalFile";
    public static final String KEY_PRINCIPAL_FILE = "principalFile";
    private static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    private static final char[] DIGITS_LOWER = {'0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private static String LOCAL_CACHE_DIR;
    static {
        String systemInfo = System.getProperty("os.name");
        if(systemInfo.toLowerCase().startsWith("windows")){
            LOCAL_CACHE_DIR = System.getProperty("user.dir");
        } else {
            LOCAL_CACHE_DIR = "/tmp/flinkx/keytab";
        }

        createDir(LOCAL_CACHE_DIR);
    }

    public static UserGroupInformation loginAndReturnUGI(Configuration conf, String principal, String keytab) throws IOException {
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
            reloadKrb5Conf(conf);
        }

        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);

        LOG.info("login user:{} with keytab:{}", principal, keytab);
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
    }

    private static void reloadKrb5Conf(Configuration conf){
        String krb5File = conf.get(KEY_JAVA_SECURITY_KRB5_CONF);
        LOG.info("set krb5 file:{}", krb5File);
        System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, krb5File);

        try {
            if (!System.getProperty("java.vendor").contains("IBM")) {
                Config.refresh();
            }
        } catch (Exception e){
            LOG.warn("reload krb5 file:{} error:", krb5File, e);
        }
    }

    public static void loadKrb5Conf(Map<String, Object> kerberosConfig){
        String krb5FilePath = MapUtils.getString(kerberosConfig, KEY_JAVA_SECURITY_KRB5_CONF);
        if(StringUtils.isEmpty(krb5FilePath)){
            LOG.info("krb5 file is empty,will use default file");
            return;
        }

        krb5FilePath = loadFile(kerberosConfig, krb5FilePath);
        kerberosConfig.put(KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);
    }

    /**
     * kerberosConfig
     * {
     *     "principalFile":"keytab.keytab",
     *     "remoteDir":"/home/admin",
     *     "sftpConf":{
     *          "path" : "/home/admin",
     *          "password" : "******",
     *          "port" : "22",
     *          "auth" : "1",
     *          "host" : "127.0.0.1",
     *          "username" : "admin"
     *     }
     * }
     */
    public static String loadFile(Map<String, Object> kerberosConfig, String filePath) {
        boolean useLocalFile = MapUtils.getBooleanValue(kerberosConfig, KEY_USE_LOCAL_FILE);
        if(useLocalFile){
            LOG.info("will use local file:{}", filePath);
            checkFileExists(filePath);
        } else {
            if(filePath.contains(SP)){
                filePath = filePath.substring(filePath.lastIndexOf(SP) + 1);
            }

            filePath = loadFromSFTP(kerberosConfig, filePath);
        }

        return filePath;
    }

    private static void checkFileExists(String filePath){
       File file = new File(filePath);
       if (file.exists()){
           if (file.isDirectory()) {
               throw new RuntimeException("keytab is a directory:" + filePath);
           }
       } else {
           throw new RuntimeException("keytab file not exists:" + filePath);
       }
    }

    private static String loadFromSFTP(Map<String, Object> config, String fileName){
        String remoteDir = MapUtils.getString(config, KEY_REMOTE_DIR);
        String filePathOnSFTP = remoteDir + "/" + fileName;

        String localDirName = getMD5(remoteDir);
        String localDir = LOCAL_CACHE_DIR + SP + localDirName;
        localDir = createDir(localDir);
        String fileLocalPath = localDir + SP + fileName;
        if (fileExists(fileLocalPath)) {
            return fileLocalPath;
        } else {
            SFTPHandler handler = null;
            try {
                handler = SFTPHandler.getInstanceWithRetry(MapUtils.getMap(config, KEY_SFTP_CONF));
                if(handler.isFileExist(filePathOnSFTP)){
                    handler.downloadFileWithRetry(filePathOnSFTP, fileLocalPath);

                    LOG.info("download file:{} to local:{}", filePathOnSFTP, fileLocalPath);
                    return fileLocalPath;
                }
            } catch (Exception e){
                throw new RuntimeException(e);
            } finally {
                if (handler != null){
                    handler.close();
                }
            }

            throw new RuntimeException("File[" + filePathOnSFTP + "] not exist on sftp");
        }
    }

    private static String getMD5(String filePath) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bytes = md5.digest(filePath.getBytes(StandardCharsets.UTF_8));
            return bytes2Hex(bytes, DIGITS_LOWER);
        } catch (Exception e) {
            throw new RuntimeException("获取本地路径出错", e);
        }
    }

    /**
     * bytes数组转16进制String
     *
     * @param data     bytes数组
     * @param toDigits DIGITS_LOWER或DIGITS_UPPER
     * @return 转化结果
     */
    private static String bytes2Hex(final byte[] data, final char[] toDigits) {
        final int l = data.length;
        final char[] out = new char[l << 1];
        // two characters form the hex value.
        for (int i = 0, j = 0; i < l; i++) {
            out[j++] = toDigits[(0xF0 & data[i]) >>> 4];
            out[j++] = toDigits[0x0F & data[i]];
        }
        return new String(out);
    }

    public static String findPrincipalFromKeytab(String keytabFile) {
        KeyTab keyTab = KeyTab.getInstance(keytabFile);
        for (KeyTabEntry entry : keyTab.getEntries()) {
            String principal = entry.getService().getName();

            LOG.info("parse principal:{} from keytab:{}", principal, keytabFile);
            return principal;
        }

        return null;
    }

    private static boolean fileExists(String filePath) {
        File file = new File(filePath);
        return file.exists() && file.isFile();
    }

    private static String createDir(String dir){
        File file = new File(dir);
        if (file.exists()){
            return dir;
        }

        boolean result = file.mkdirs();
        if (!result){
            LOG.warn("Create dir failure:{}", dir);
        }

        LOG.info("create local dir:{}", dir);
        return dir;
    }

    public static String getPrincipalFileName(Map<String, Object> config) {
        String fileName = MapUtils.getString(config, "principalFile");
        if (StringUtils.isEmpty(fileName)) {
            throw new RuntimeException("[principalFile]必须指定");
        }

        if (fileName.contains(SP)) {
            fileName = fileName.substring(fileName.lastIndexOf(SP) + 1);
        }

        return fileName;
    }
}
