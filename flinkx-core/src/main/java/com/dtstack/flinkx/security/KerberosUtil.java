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
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.FileSystemUtil;
import com.dtstack.flinkx.util.JsonUtil;
import com.dtstack.flinkx.util.Md5Util;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.runtime.security.DynamicConfiguration;
import org.apache.flink.runtime.security.KerberosUtils;

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

import javax.security.auth.login.AppConfigurationEntry;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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

    public static UserGroupInformation loginAndReturnUgi(Map<String, Object> hadoopConfig) {
        String keytabFileName = KerberosUtil.getPrincipalFileName(hadoopConfig);
        keytabFileName = KerberosUtil.loadFile(hadoopConfig, keytabFileName);

        String principal = KerberosUtil.getPrincipal(hadoopConfig, keytabFileName);
        KerberosUtil.loadKrb5Conf(hadoopConfig);

        Configuration conf = FileSystemUtil.getConfiguration(hadoopConfig, null);

        UserGroupInformation ugi;
        try {
            ugi = KerberosUtil.loginAndReturnUgi(conf, principal, keytabFileName);
        } catch (Exception e) {
            throw new RuntimeException("Login kerberos error:", e);
        }

        LOG.info("current ugi:{}", ugi);

        return ugi;
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

        LOG.info("login user:{} with keytab:{}", principal, keytab);
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
        LOG.info("login user:{} with keytab:{}", principal, keytab);
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
            Map<String, Object> kerberosConfig, DistributedCache distributedCache, String jobId) {
        String krb5FilePath = MapUtils.getString(kerberosConfig, KEY_JAVA_SECURITY_KRB5_CONF);
        if (StringUtils.isEmpty(krb5FilePath)) {
            LOG.info("krb5 file is empty,will use default file");
            return;
        }

        krb5FilePath = loadFile(kerberosConfig, krb5FilePath, distributedCache, jobId);
        kerberosConfig.put(KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);
        System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, krb5FilePath);
    }

    public static void loadKrb5Conf(Map<String, Object> kerberosConfig) {
        loadKrb5Conf(kerberosConfig, null, null);
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
            String jobId) {
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

            fileName = loadFromSftp(kerberosConfig, fileName, jobId);
            return fileName;
        }
    }

    public static String loadFile(Map<String, Object> kerberosConfig, String filePath) {
        return loadFile(kerberosConfig, filePath, null, null);
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

    private static String loadFromSftp(Map<String, Object> config, String fileName, String jobId) {
        String remoteDir = MapUtils.getString(config, KEY_REMOTE_DIR);
        if (StringUtils.isBlank(remoteDir)) {
            throw new FlinkxRuntimeException(
                    "can't find [remoteDir] in config: \n" + JsonUtil.toPrintJson(config));
        }
        String filePathOnSftp = remoteDir + "/" + fileName;
        if (null == jobId) {
            // 创建分片在 JobManager， 此时还没有 JobId，随机生成UUID
            jobId = UUID.randomUUID().toString();
            LOG.warn("jobId is null, jobId will be replaced with [UUID], jobId(UUID) = {}.", jobId);
        }
        String localDirName = Md5Util.getMd5(remoteDir);
        String localDir = LOCAL_CACHE_DIR + SP + jobId + SP + localDirName;
        // 创建 TM 节点的本地目录 /tmp/${user}/flinkx/${jobId}/${md5(remoteDir)}/*.keytab
        // 需要考虑的多种情况：
        // ① ${user} 解决不同用户的权限问题。/tmp linux 下是有 777 权限。
        // ② ${jobId} 解决多个任务在同一个 TM 上面，keytab 覆盖问题。
        // ③ ${md5(remoteDir)} 解决 reader writer 在同一个 TM 上面，keytab 覆盖问题。
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
            LOG.warn(
                    "resetting default realm failed, current default realm will still be used.", e);
        }
    }

    /** 添加或者更新JaasConfiguration */
    public static synchronized DynamicConfiguration appendOrUpdateJaasConf(
            String name, String keytab, String principal) {
        if (hasExistsOnConfiguration(name)) {
            return KerberosUtil.resetJaasConfForName(name, keytab, principal);
        } else {
            return KerberosUtil.appendJaasConf(name, keytab, principal);
        }
    }

    /** configuration里是否添加了configureName的配置 */
    public static synchronized boolean hasExistsOnConfiguration(String configureName) {
        javax.security.auth.login.Configuration priorConfig =
                javax.security.auth.login.Configuration.getConfiguration();
        AppConfigurationEntry[] appConfigurationEntry =
                priorConfig.getAppConfigurationEntry(configureName);
        return appConfigurationEntry != null;
    }

    /**
     * 反射更新 DynamicConfiguration#dynamicEntries的值
     *
     * @param name
     * @param keytab
     * @param principal
     * @return
     */
    public static DynamicConfiguration resetJaasConfForName(
            String name, String keytab, String principal) {
        LOG.info("resetJaasConfForName, name {} principal {} ,keytab {}", name, principal, keytab);
        javax.security.auth.login.Configuration config =
                javax.security.auth.login.Configuration.getConfiguration();
        if (config instanceof DynamicConfiguration) {
            Class dynamicConfigurationClass = config.getClass();
            try {
                Field dynamicEntriesField =
                        dynamicConfigurationClass.getDeclaredField("dynamicEntries");
                dynamicEntriesField.setAccessible(true);
                Map<String, AppConfigurationEntry[]> dynamicEntries =
                        (Map<String, AppConfigurationEntry[]>) dynamicEntriesField.get(config);
                Map<String, AppConfigurationEntry[]> newDynamicEntries = new HashMap<>();

                // 存在相同的Name时 直接用当前的覆盖以前的
                dynamicEntries.forEach(
                        (k, v) -> {
                            if (k.equals(name)) {
                                AppConfigurationEntry krb5Entry =
                                        KerberosUtils.keytabEntry(keytab, principal);
                                AppConfigurationEntry[] appConfigurationEntries =
                                        new AppConfigurationEntry[1];
                                appConfigurationEntries[0] = krb5Entry;
                                newDynamicEntries.put(name, appConfigurationEntries);
                            } else {
                                newDynamicEntries.put(k, v);
                            }
                        });

                dynamicEntriesField.set(config, newDynamicEntries);
            } catch (Exception e) {
                LOG.warn("reflex error", e);
                return appendJaasConf(name, keytab, principal);
            }
            return (DynamicConfiguration) config;
        } else {
            return appendJaasConf(name, keytab, principal);
        }
    }

    public static DynamicConfiguration appendJaasConf(
            String name, String keytab, String principal) {
        LOG.info("appendJaasConf, name {} principal {} ,keytab {}", name, principal, keytab);
        javax.security.auth.login.Configuration priorConfig =
                javax.security.auth.login.Configuration.getConfiguration();
        // construct a dynamic JAAS configuration
        DynamicConfiguration currentConfig = new DynamicConfiguration(priorConfig);
        // wire up the configured JAAS login contexts to use the krb5 entries
        AppConfigurationEntry krb5Entry = KerberosUtils.keytabEntry(keytab, principal);
        currentConfig.addAppConfigurationEntry(name, krb5Entry);
        javax.security.auth.login.Configuration.setConfiguration(currentConfig);
        return currentConfig;
    }

    /**
     * 获取filePath的本地路径
     *
     * @param kerberosConfig
     * @param filePath
     * @param distributedCache
     * @param jobId
     * @return
     */
    public static String getLocalFileName(
            Map<String, Object> kerberosConfig,
            String filePath,
            DistributedCache distributedCache,
            String jobId) {
        boolean useLocalFile = MapUtils.getBooleanValue(kerberosConfig, KEY_USE_LOCAL_FILE);
        if (useLocalFile) {
            LOG.info("will use local file:{}", filePath);
            checkFileExists(filePath);
            return filePath;
        } else {
            String fileName = filePath;
            if (filePath.contains(SP)) {
                fileName = filePath.substring(filePath.lastIndexOf(SP) + 1);
            }
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
                            ExceptionUtil.getErrorMessage(e));
                }
            }

            String remoteDir = MapUtils.getString(kerberosConfig, KEY_REMOTE_DIR);
            if (null == jobId) {
                LOG.warn("jobId is null, jobId will be replaced with [local].");
                jobId = "local";
            }
            String localDirName = Md5Util.getMd5(remoteDir);
            String localDir = LOCAL_CACHE_DIR + SP + jobId + SP + localDirName + SP + fileName;

            return localDir;
        }
    }
}
