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

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.util.Md5Util;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;
import sun.security.krb5.internal.ktab.KeyTab;
import sun.security.krb5.internal.ktab.KeyTabEntry;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author jiangbo
 * @date 2019/8/20
 */
public class KerberosUtil {

    public static Logger LOG = LoggerFactory.getLogger(KerberosUtil.class);

    private static final String SP = "/";

    private static final String KEY_SFTP_CONF = "sftpConf";
    private static final String KEY_PRINCIPAL = "principal";
    private static final String KEY_REMOTE_DIR = "remoteDir";
    private static final String KEY_USE_LOCAL_FILE = "useLocalFile";
    public static final String KEY_PRINCIPAL_FILE = "principalFile";
    private static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    private static final String JAVA_VENDOR_IBM = "IBM";

    private static String LOCAL_CACHE_DIR;
    static {
        String systemInfo = System.getProperty(ConstantValue.SYSTEM_PROPERTIES_KEY_OS);
        if(systemInfo.toLowerCase().startsWith(ConstantValue.OS_WINDOWS)){
            LOCAL_CACHE_DIR = System.getProperty(ConstantValue.SYSTEM_PROPERTIES_KEY_USER_DIR);
        } else {
            LOCAL_CACHE_DIR = "/tmp/flinkx/keytab";
        }

        createDir(LOCAL_CACHE_DIR);
    }

    public static UserGroupInformation loginAndReturnUgi(Configuration conf, String principal, String keytab) throws IOException {
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

    public static String getPrincipal(Map<String,Object> configMap, String keytabPath) {
        String principal = MapUtils.getString(configMap, KEY_PRINCIPAL);
        if (StringUtils.isEmpty(principal)) {
            principal = findPrincipalFromKeytab(keytabPath);
        }

        return principal;
    }

    private static void reloadKrb5Conf(Configuration conf){
        String krb5File = conf.get(KEY_JAVA_SECURITY_KRB5_CONF);
        LOG.info("set krb5 file:{}", krb5File);
        System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, krb5File);

        try {
            if (!System.getProperty(ConstantValue.SYSTEM_PROPERTIES_KEY_JAVA_VENDOR).contains(JAVA_VENDOR_IBM)) {
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
        System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);
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

            filePath = loadFromSftp(kerberosConfig, filePath);
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

    private static String loadFromSftp(Map<String, Object> config, String fileName){
        String remoteDir = MapUtils.getString(config, KEY_REMOTE_DIR);
        String filePathOnSftp = remoteDir + "/" + fileName;

        String localDirName = Md5Util.getMd5(remoteDir);
        String localDir = LOCAL_CACHE_DIR + SP + localDirName;
        localDir = createDir(localDir);
        String fileLocalPath = localDir + SP + fileName;
        // 更新sftp文件对应的local文件
        if (fileExists(fileLocalPath)) {
            delectFile(fileLocalPath);
        }
        SftpHandler handler = null;
        try {
            handler = SftpHandler.getInstanceWithRetry(MapUtils.getMap(config, KEY_SFTP_CONF));
            if(handler.isFileExist(filePathOnSftp)){
                handler.downloadFileWithRetry(filePathOnSftp, fileLocalPath);

                LOG.info("download file:{} to local:{}", filePathOnSftp, fileLocalPath);
                return fileLocalPath;
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            if (handler != null){
                handler.close();
            }
        }

        throw new RuntimeException("File[" + filePathOnSftp + "] not exist on sftp");
    }

    private static String findPrincipalFromKeytab(String keytabFile) {
        KeyTab keyTab = KeyTab.getInstance(keytabFile);
        for (KeyTabEntry entry : keyTab.getEntries()) {
            String principal = entry.getService().getName();

            LOG.info("parse principal:{} from keytab:{}", principal, keytabFile);
            return principal;
        }

        return null;
    }

    private static void delectFile(String filePath){
        if (fileExists(filePath)) {
            File file = new File(filePath);
            if(file.delete()){
                LOG.info(file.getName() + " is delected！");
            }else{
                LOG.error("delected " + file.getName() + " failed！");
            }
        }
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

        return fileName;
    }

    /**
     * 刷新krb内容信息
     */
    public static void refreshConfig() {
        try{
            sun.security.krb5.Config.refresh();
            Field defaultRealmField = KerberosName.class.getDeclaredField("defaultRealm");
            defaultRealmField.setAccessible(true);
            defaultRealmField.set(null, org.apache.hadoop.security.authentication.util.KerberosUtil.getDefaultRealm());
            //reload java.security.auth.login.config
            javax.security.auth.login.Configuration.setConfiguration(null);
        }catch (Exception e){
            LOG.warn("resetting default realm failed, current default realm will still be used.", e);
        }

    }
}
