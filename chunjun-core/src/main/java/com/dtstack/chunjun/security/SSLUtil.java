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

package com.dtstack.chunjun.security;

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.Md5Util;

import org.apache.flink.api.common.cache.DistributedCache;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Map;

@Slf4j
public class SSLUtil {

    private static final String SP = "/";

    private static final String KEY_SFTP_CONF = "sftpConf";

    private static final String KEY_PATH = "path";

    private static final String KEY_USE_LOCAL_FILE = "useLocalFile";

    private static final String KEY_PKCS12 = "pkcs12";

    private static final String KEY_CA = "ca";

    private static final String LOCAL_CACHE_DIR;

    static {
        String systemInfo = System.getProperty(ConstantValue.SYSTEM_PROPERTIES_KEY_OS);
        if (systemInfo.toLowerCase().startsWith(ConstantValue.OS_WINDOWS)) {
            LOCAL_CACHE_DIR = System.getProperty(ConstantValue.SYSTEM_PROPERTIES_KEY_USER_DIR);
        } else {
            LOCAL_CACHE_DIR = "/tmp/chunjun/ssl";
        }

        createDir(LOCAL_CACHE_DIR);
    }

    /**
     * "sslConfig": { "useLocalFile"：boolean值，为true则使用本地文件，false表示使用远程目录文件 "fileName"：String值，文件名
     * "filePath"：String值，文件路径 "type": String值，证书类型格式；pkcs12或ca "keyStorePass": String值，使用证书文件的密码
     * "sftpConf": { "path":"/data/sftp", "password":"password", "port":"22", "auth":"1",
     * "host":"127.0.0.1", "username":"root" } }
     */
    public static String loadFile(
            Map<String, Object> sslConfig, String filePath, DistributedCache distributedCache) {
        boolean useLocalFile = MapUtils.getBooleanValue(sslConfig, KEY_USE_LOCAL_FILE);
        if (useLocalFile) {
            log.info("will use local file:{}", filePath);
            checkFileExists(filePath);
            return filePath;
        } else {
            String fileName = filePath;
            if (filePath.contains(SP)) {
                fileName = filePath.substring(filePath.lastIndexOf(SP) + 1);
            }
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
                            ExceptionUtil.getErrorMessage(e));
                }
            }

            fileName = loadFromSftp(MapUtils.getMap(sslConfig, KEY_SFTP_CONF), fileName);
            return fileName;
        }
    }

    /**
     * 根据认证方式获取KeyStore
     *
     * @param type ssl证书格式
     * @param path 证书文件路径
     * @param keyStorePass 使用证书文件的密码
     * @return KeyStore
     */
    public static KeyStore getKeyStoreByType(String type, Path path, String keyStorePass) {
        KeyStore keyStore = null;
        InputStream is = null;
        try {
            if (KEY_PKCS12.equalsIgnoreCase(type)) {
                log.info("init RestClient, type: pkcs#12.");
                keyStore = KeyStore.getInstance("pkcs12");
                is = Files.newInputStream(path);
                keyStore.load(is, keyStorePass.toCharArray());
            } else if (KEY_CA.equalsIgnoreCase(type)) {
                log.info(
                        "init RestClient, type: use CA certificate that is available as a PEM encoded file.");
                CertificateFactory factory = CertificateFactory.getInstance("X.509");
                Certificate trustedCa;
                is = Files.newInputStream(path);
                trustedCa = factory.generateCertificate(is);
                keyStore = KeyStore.getInstance("pkcs12");
                keyStore.load(null, null);
                keyStore.setCertificateEntry("ca", trustedCa);
            } else {
                throw new UnsupportedOperationException("can not support this type : " + type);
            }
            return keyStore;
        } catch (IOException
                | KeyStoreException
                | NoSuchAlgorithmException
                | CertificateException e) {
            throw new RuntimeException(e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (final IOException ignored) { // NOPMD
                }
            }
        }
    }

    private static String loadFromSftp(Map<String, String> config, String fileName) {
        String remoteDir = MapUtils.getString(config, KEY_PATH);
        String filePathOnSftp = remoteDir + "/" + fileName;

        String localDirName = Md5Util.getMd5(remoteDir);
        String localDir = LOCAL_CACHE_DIR + SP + localDirName;
        localDir = createDir(localDir);
        String fileLocalPath = localDir + SP + fileName;
        // 更新sftp文件对应的local文件
        if (fileExists(fileLocalPath)) {
            deleteFile(fileLocalPath);
        }
        SftpHandler handler = null;
        try {
            handler = SftpHandler.getInstanceWithRetry(config);
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

    protected static void checkFileExists(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new RuntimeException(filePath + " is a directory.");
            }
        } else {
            throw new RuntimeException(filePath + " not exists.");
        }
    }

    private static void deleteFile(String filePath) {
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

    private static String createDir(String dir) {
        File file = new File(dir);
        if (file.exists()) {
            return dir;
        }

        boolean result = file.mkdirs();
        if (!result) {
            throw new RuntimeException("create dir failure: " + dir);
        }

        log.info("create local dir:{}", dir);
        return dir;
    }
}
