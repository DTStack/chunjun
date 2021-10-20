/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.security;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.JsonUtil;
import com.dtstack.flinkx.util.Md5Util;

import org.apache.flink.api.common.cache.DistributedCache;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;
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
    public static final String KRB_STR = "Kerberos";
    public static final String HADOOP_AUTH_KEY = "hadoop.security.authentication";
    public static final String KRB5_CONF_KEY = "java.security.krb5.conf";

    private static final String LOCAL_CACHE_DIR;

    static {
        String systemInfo = System.getProperty(ConstantValue.SYSTEM_PROPERTIES_KEY_OS);
        if (systemInfo.toLowerCase().startsWith(ConstantValue.OS_WINDOWS)) {
            LOCAL_CACHE_DIR = System.getProperty(ConstantValue.SYSTEM_PROPERTIES_KEY_USER_DIR);
        } else {
            LOCAL_CACHE_DIR = "/tmp/flinkx/keytab";
        }

        createDir(LOCAL_CACHE_DIR);
    }

    public static UserGroupInformation loginAndReturnUgi(KerberosConfig kerberosConfig)
            throws IOException {
        String principal = kerberosConfig.getPrincipal();
        String keytabPath = kerberosConfig.getKeytab();
        String krb5confPath = kerberosConfig.getKrb5conf();
        LOG.info("Kerberos login with principal: {} and keytab: {}", principal, keytabPath);
        return loginAndReturnUgi(principal, keytabPath, krb5confPath);
    }

    public static UserGroupInformation loginAndReturnUgi(
            String principal, String keytab, String krb5Conf) throws IOException {
        if (StringUtils.isEmpty(principal)) {
            throw new IllegalArgumentException("principal can not be null");
        }

        if (StringUtils.isEmpty(keytab)) {
            throw new IllegalArgumentException("keytab can not be null");
        }

        if (StringUtils.isNotEmpty(krb5Conf)) {
            reloadKrb5conf(krb5Conf);
        }
        Configuration conf = new Configuration();
        conf.set(HADOOP_AUTH_KEY, KRB_STR);
        UserGroupInformation.setConfiguration(conf);
        LOG.info("login user:{} with keytab:{}", principal, keytab);
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
    }

    public static String getKrb5Conf(Map<String, Object> configMap) {
        String krb5Conf = MapUtils.getString(configMap, KRB5_CONF_KEY);
        if (StringUtils.isEmpty(krb5Conf)) {
            throw new RuntimeException("[java.security.krb5.conf]必须指定");
        }

        return krb5Conf;
    }

    public static String getPrincipal(Map<String, Object> configMap, String keytabPath) {
        String principal = MapUtils.getString(configMap, KEY_PRINCIPAL);
        if (StringUtils.isEmpty(principal)) {
            principal = findPrincipalFromKeytab(keytabPath);
        }

        return principal;
    }

    public static String getPrincipalFileName(Map<String, Object> config) {
        String fileName = MapUtils.getString(config, KEY_PRINCIPAL_FILE);
        if (StringUtils.isEmpty(fileName)) {
            throw new RuntimeException("[principalFile]必须指定");
        }

        return fileName;
    }

    public static synchronized void reloadKrb5conf(String krb5confPath) {
        System.setProperty(KRB5_CONF_KEY, krb5confPath);
        LOG.info("set krb5 file:{}", krb5confPath);
        // 不刷新会读/etc/krb5.conf
        try {
            Config.refresh();
            KerberosName.resetDefaultRealm();
        } catch (KrbException e) {
            LOG.warn(
                    "resetting default realm failed, current default realm will still be used.", e);
        }
    }

    public static void loadKrb5Conf(
            Map<String, Object> kerberosConfig, DistributedCache distributedCache) {
        String krb5FilePath = MapUtils.getString(kerberosConfig, KEY_JAVA_SECURITY_KRB5_CONF);
        if (StringUtils.isEmpty(krb5FilePath)) {
            LOG.info("krb5 file is empty,will use default file");
            return;
        }

        krb5FilePath = loadFile(kerberosConfig, krb5FilePath, distributedCache);
        kerberosConfig.put(KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);
        System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);
    }

    public static void loadKrb5Conf(Map<String, Object> kerberosConfig) {
        loadKrb5Conf(kerberosConfig, null);
    }

    /**
     * kerberosConfig { "principalFile":"keytab.keytab", "remoteDir":"/home/admin", "sftpConf":{
     * "path" : "/home/admin", "password" : "******", "port" : "22", "auth" : "1", "host" :
     * "127.0.0.1", "username" : "admin" } }
     */
    public static String loadFile(
            Map<String, Object> kerberosConfig,
            String filePath,
            DistributedCache distributedCache) {
        boolean useLocalFile = MapUtils.getBooleanValue(kerberosConfig, KEY_USE_LOCAL_FILE);
        if (useLocalFile) {
            LOG.info("will use local file:{}", filePath);
            checkFileExists(filePath);
            return filePath;
        } else {
            String fileName = new File(filePath).getName();
            if (StringUtils.startsWith(fileName, "blob_")) {
                // already downloaded from blobServer
                LOG.info("file [{}] already downloaded from blobServer", filePath);
                return filePath;
            }
            if (distributedCache != null) {
                try {
                    File file = distributedCache.getFile(fileName);
                    String absolutePath = file.getAbsolutePath();
                    LOG.info(
                            "load file [{}] from Flink BlobServer, download file path = {}",
                            fileName,
                            absolutePath);
                    return absolutePath;
                } catch (Exception e) {
                    LOG.warn(
                            "failed to get [{}] from Flink BlobServer, try to get from sftp. e = {}",
                            fileName,
                            e.getMessage());
                }
            }

            fileName = loadFromSftp(kerberosConfig, fileName);
            return fileName;
        }
    }

    public static String loadFile(Map<String, Object> kerberosConfig, String filePath) {
        return loadFile(kerberosConfig, filePath, null);
    }

    private static void checkFileExists(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new RuntimeException("keytab is a directory:" + filePath);
            }
        } else {
            throw new RuntimeException("keytab file not exists:" + filePath);
        }
    }

    private static String loadFromSftp(Map<String, Object> config, String fileName) {
        String remoteDir = MapUtils.getString(config, KEY_REMOTE_DIR);
        if (StringUtils.isBlank(remoteDir)) {
            throw new FlinkxRuntimeException(
                    "can't find [remoteDir] in config: \n" + JsonUtil.toPrintJson(config));
        }
        String filePathOnSftp = remoteDir + "/" + fileName;

        String localDirName = Md5Util.getMd5(remoteDir);
        String localDir = LOCAL_CACHE_DIR + SP + localDirName;
        localDir = createDir(localDir);
        String fileLocalPath = localDir + SP + fileName;
        // 更新sftp文件对应的local文件
        if (fileExists(fileLocalPath)) {
            detectFile(fileLocalPath);
        }
        SftpHandler handler = null;
        try {
            handler = SftpHandler.getInstanceWithRetry(MapUtils.getMap(config, KEY_SFTP_CONF));
            if (handler.isFileExist(filePathOnSftp)) {
                handler.downloadFileWithRetry(filePathOnSftp, fileLocalPath);

                LOG.info("download file:{} to local:{}", filePathOnSftp, fileLocalPath);
                return fileLocalPath;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (handler != null) {
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

    private static void detectFile(String filePath) {
        if (fileExists(filePath)) {
            File file = new File(filePath);
            if (file.delete()) {
                LOG.info(file.getName() + " is deleted！");
            } else {
                LOG.error("deleted " + file.getName() + " failed！");
            }
        }
    }

    private static boolean fileExists(String filePath) {
        File file = new File(filePath);
        return file.exists() && file.isFile();
    }

    private static String createDir(String dir) {
        File file = new File(dir);
        if (file.exists()) {
            return dir;
        }

        boolean result = file.mkdirs();
        if (!result) {
            LOG.warn("Create dir failure:{}", dir);
        }

        LOG.info("create local dir:{}", dir);
        return dir;
    }


    /** 刷新krb内容信息 */
    public static void refreshConfig() {
        try {
            sun.security.krb5.Config.refresh();
            Field defaultRealmField = KerberosName.class.getDeclaredField("defaultRealm");
            defaultRealmField.setAccessible(true);
            defaultRealmField.set(
                    null,
                    org.apache.hadoop.security.authentication.util.KerberosUtil.getDefaultRealm());
            // reload java.security.auth.login.config
            javax.security.auth.login.Configuration.setConfiguration(null);
        } catch (Exception e) {
            LOG.warn(
                    "resetting default realm failed, current default realm will still be used.", e);
        }
    }
}
