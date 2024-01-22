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

package com.dtstack.chunjun.security;

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.FileSystemUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.Md5Util;

import org.apache.flink.api.common.cache.DistributedCache;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;
import sun.security.krb5.internal.ktab.KeyTab;
import sun.security.krb5.internal.ktab.KeyTabEntry;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class KerberosUtil {

    private static final String SP = "/";

    private static final String KEY_SFTP_CONF = "sftpConf";
    public static final String KEY_PRINCIPAL = "principal";
    public static final String KEY_REMOTE_DIR = "remoteDir";
    public static final String KEY_USE_LOCAL_FILE = "useLocalFile";
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
            LOCAL_CACHE_DIR = "/tmp/chunjun/keytab";
        }

        createDir(LOCAL_CACHE_DIR);
    }

    public static UserGroupInformation loginAndReturnUgi(
            Map<String, Object> hadoopConfig, String jobId, String taskNumber) {
        String keytabFileName = KerberosUtil.getPrincipalFileName(hadoopConfig);
        keytabFileName = KerberosUtil.loadFile(hadoopConfig, keytabFileName, jobId, taskNumber);

        String principal = KerberosUtil.getPrincipal(hadoopConfig, keytabFileName);
        KerberosUtil.loadKrb5Conf(hadoopConfig, null, jobId, taskNumber);

        Configuration conf = FileSystemUtil.getConfiguration(hadoopConfig, null);

        UserGroupInformation ugi;
        try {
            ugi = KerberosUtil.loginAndReturnUgi(conf, principal, keytabFileName);
        } catch (Exception e) {
            throw new RuntimeException("Login kerberos error:", e);
        }

        log.info("current ugi:{}", ugi);

        return ugi;
    }

    public static UserGroupInformation loginAndReturnUgi(KerberosConfig kerberosConfig)
            throws IOException {
        String principal = kerberosConfig.getPrincipal();
        String keytabPath = kerberosConfig.getKeytab();
        String krb5confPath = kerberosConfig.getKrb5conf();
        log.info("Kerberos login with principal: {} and keytab: {}", principal, keytabPath);
        return loginAndReturnUgi(principal, keytabPath, krb5confPath);
    }

    public static UserGroupInformation loginAndReturnUgi(
            Configuration conf, String principal, String keytab) throws IOException {
        if (conf == null) {
            throw new IllegalArgumentException("kerberos conf can not be null");
        }

        if (StringUtils.isEmpty(principal)) {
            throw new IllegalArgumentException("principal can not be null");
        }

        if (StringUtils.isEmpty(keytab)) {
            throw new IllegalArgumentException("keytab can not be null");
        }
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);

        log.info("login user:{} with keytab:{}", principal, keytab);
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
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
        log.info("login user:{} with keytab:{}", principal, keytab);
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
    }

    public static String getPrincipal(Map<String, Object> configMap, String keytabPath) {
        String principal = MapUtils.getString(configMap, KEY_PRINCIPAL);
        if (StringUtils.isEmpty(principal)) {
            principal = findPrincipalFromKeytab(keytabPath);
        }

        return principal;
    }

    public static synchronized void reloadKrb5conf(String krb5confPath) {
        System.setProperty(KRB5_CONF_KEY, krb5confPath);
        log.info("set krb5 file:{}", krb5confPath);
        // 不刷新会读/etc/krb5.conf
        try {
            Config.refresh();
            KerberosName.resetDefaultRealm();
        } catch (KrbException e) {
            log.warn(
                    "resetting default realm failed, current default realm will still be used.", e);
        }
    }

    public static void loadKrb5Conf(
            Map<String, Object> kerberosConfig,
            DistributedCache distributedCache,
            String jobId,
            String taskNumber) {
        String krb5FilePath = MapUtils.getString(kerberosConfig, KEY_JAVA_SECURITY_KRB5_CONF);
        if (StringUtils.isEmpty(krb5FilePath)) {
            log.info("krb5 file is empty,will use default file");
            return;
        }

        krb5FilePath = loadFile(kerberosConfig, krb5FilePath, distributedCache, jobId, taskNumber);
        kerberosConfig.put(KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);
        System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);
    }

    public static void loadKrb5Conf(
            Map<String, Object> kerberosConfig, String jobId, String taskNumber) {
        loadKrb5Conf(kerberosConfig, null, jobId, taskNumber);
    }

    /**
     * kerberosConfig { "principalFile":"keytab.keytab", "remoteDir":"/home/admin", "sftpConf":{
     * "path" : "/home/admin", "password" : "******", "port" : "22", "auth" : "1", "host" :
     * "127.0.0.1", "username" : "admin" } }
     */
    public static String loadFile(
            Map<String, Object> kerberosConfig,
            String filePath,
            DistributedCache distributedCache,
            String jobId,
            String taskNumber) {
        boolean useLocalFile = MapUtils.getBooleanValue(kerberosConfig, KEY_USE_LOCAL_FILE);
        if (useLocalFile) {
            log.info("will use local file:{}", filePath);
            checkFileExists(filePath);
            return filePath;
        } else {
            String fileName = new File(filePath).getName();
            if (StringUtils.startsWith(fileName, "blob_")) {
                // already downloaded from blobServer
                log.info("file [{}] already downloaded from blobServer", filePath);
                return filePath;
            }
            if (distributedCache != null) {
                try {
                    File file = distributedCache.getFile(fileName);
                    String absolutePath = file.getAbsolutePath();
                    log.info(
                            "load file [{}] from Flink BlobServer, download file path = {}",
                            fileName,
                            absolutePath);
                    return absolutePath;
                } catch (Exception e) {
                    log.warn(
                            "failed to get [{}] from Flink BlobServer, try to get from sftp. e = {}",
                            fileName,
                            e.getMessage());
                }
            }

            fileName = loadFromSftp(kerberosConfig, fileName, jobId, taskNumber);
            return fileName;
        }
    }

    public static String loadFile(
            Map<String, Object> kerberosConfig, String filePath, String jobId, String taskNumber) {
        return loadFile(kerberosConfig, filePath, null, jobId, taskNumber);
    }

    public static void checkFileExists(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new RuntimeException("keytab is a directory:" + filePath);
            }
        } else {
            throw new RuntimeException("keytab file not exists:" + filePath);
        }
    }

    private static String loadFromSftp(
            Map<String, Object> config, String fileName, String jobId, String taskNumber) {
        String remoteDir = MapUtils.getString(config, KEY_REMOTE_DIR);
        if (StringUtils.isBlank(remoteDir)) {
            throw new ChunJunRuntimeException(
                    "can't find [remoteDir] in config: \n" + JsonUtil.toPrintJson(config));
        }
        String filePathOnSftp = remoteDir + "/" + fileName;

        if (StringUtils.isBlank(jobId)) {
            // 创建分片在 JobManager， 此时还没有 JobId，随机生成UUID
            jobId = UUID.randomUUID().toString();
            log.warn("jobId is null, jobId will be replaced with [UUID], jobId(UUID) = {}.", jobId);
        }

        if (StringUtils.isBlank(taskNumber)) {
            taskNumber = UUID.randomUUID().toString();
            log.warn(
                    "taskNumber is null, taskNumber will be replaced with [UUID], taskNumber(UUID) = {}.",
                    taskNumber);
        }

        String localDirName = Md5Util.getMd5(remoteDir);
        String localDir = LOCAL_CACHE_DIR + SP + jobId + SP + taskNumber + SP + localDirName;
        localDir = createDir(localDir);
        String fileLocalPath = localDir + SP + fileName;
        // 更新sftp文件对应的local文件
        if (fileExists(fileLocalPath)) {
            detectFile(fileLocalPath);
        }
        SftpHandler handler = null;
        try {
            handler =
                    SftpHandler.getInstanceWithRetry(
                            GsonUtil.GSON.fromJson(
                                    MapUtils.getString(config, KEY_SFTP_CONF), Map.class));
            if (handler.isFileExist(filePathOnSftp)) {
                handler.downloadFileWithRetry(filePathOnSftp, fileLocalPath);

                log.info("download file:{} to local:{}", filePathOnSftp, fileLocalPath);
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

    protected static String findPrincipalFromKeytab(String keytabFile) {
        KeyTab keyTab = KeyTab.getInstance(keytabFile);
        for (KeyTabEntry entry : keyTab.getEntries()) {
            String principal = entry.getService().getName();

            log.info("parse principal:{} from keytab:{}", principal, keytabFile);
            return principal;
        }

        return null;
    }

    private static void detectFile(String filePath) {
        if (fileExists(filePath)) {
            File file = new File(filePath);
            if (file.delete()) {
                log.info(file.getName() + " is deleted！");
            } else {
                log.error("deleted " + file.getName() + " failed！");
            }
        }
    }

    protected static boolean fileExists(String filePath) {
        File file = new File(filePath);
        return file.exists() && file.isFile();
    }

    protected static String createDir(String dir) {
        File file = new File(dir);
        if (file.exists()) {
            return dir;
        }

        boolean result = file.mkdirs();
        if (!result) {
            log.warn("Create dir failure:{}", dir);
        }

        log.info("create local dir:{}", dir);
        return dir;
    }

    public static String getPrincipalFileName(Map<String, Object> config) {
        String fileName = MapUtils.getString(config, "principalFile");
        if (StringUtils.isEmpty(fileName)) {
            throw new RuntimeException("[principalFile]必须指定");
        }

        return fileName;
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
            log.warn(
                    "resetting default realm failed, current default realm will still be used.", e);
        }
    }
}
