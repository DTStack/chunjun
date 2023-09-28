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

import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.fs.Path;

import com.google.common.util.concurrent.Futures;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.dtstack.chunjun.security.KerberosUtil.KEY_USE_LOCAL_FILE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KerberosUtilTest {

    private static final String TEST_PRINCIPAL = "hdfs@CHUNJUN.COM";

    @TempDir static File kdcDir;

    private static MiniKdc kdc;

    private static File keytab;

    @BeforeAll
    static void beforeAll() throws Exception {
        setUpMiniKdc();
        // resetting kerberos security
        Configuration conf = new Configuration();
        UserGroupInformation.setConfiguration(conf);
        KerberosName.resetDefaultRealm();
    }

    @AfterAll
    static void afterAll() {
        kdc.stop();
        // reset authentication type to simple otherwise it will affect other test classes
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "simple");
        UserGroupInformation.setConfiguration(conf);
    }

    private static void setUpMiniKdc() throws Exception {
        Properties kdcConf = MiniKdc.createConf();
        kdcConf.put("org.name", "CHUNJUN");
        setUpMiniKdc(kdcConf);
    }

    private static void setUpMiniKdc(Properties kdcConf) throws Exception {
        kdc = new MiniKdc(kdcConf, kdcDir);
        kdc.start();
        keytab = new File(kdcDir, "keytab");
        List<String> principals = new ArrayList<>();
        principals.add("hdfs");
        for (Type type : Type.values()) {
            principals.add(type.toString());
        }
        principals.add("CREATE_MATERIAL");
        principals.add("ROLLOVER_MATERIAL");
        kdc.createPrincipal(keytab, principals.toArray(new String[0]));
    }

    public enum Type {
        CREATE,
        DELETE,
        ROLLOVER,
        GET,
        GET_KEYS,
        GET_METADATA,
        SET_KEY_MATERIAL,
        GENERATE_EEK,
        DECRYPT_EEK;
    }

    @Test
    public void testRefreshConfig()
            throws IOException, ClassNotFoundException, NoSuchMethodException,
                    InvocationTargetException, IllegalAccessException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        StringBuilder sb = new StringBuilder();
        InputStream is2 = cl.getResourceAsStream("minikdc-krb5.conf");
        BufferedReader r = null;

        try {
            r = new BufferedReader(new InputStreamReader(is2, Charsets.UTF_8));

            for (String line = r.readLine(); line != null; line = r.readLine()) {
                sb.append(line).append("{3}");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(r);
            IOUtils.closeQuietly(is2);
        }
        String errorRealm =
                "CHUNJUN1".toUpperCase(Locale.ENGLISH) + "." + "COM".toUpperCase(Locale.ENGLISH);
        File errorKrb5file = new File(kdcDir.toString() + "/errorKrb5.conf");
        FileUtils.writeStringToFile(
                errorKrb5file,
                MessageFormat.format(
                        sb.toString(),
                        errorRealm,
                        kdc.getHost(),
                        Integer.toString(kdc.getPort()),
                        System.getProperty("line.separator")));
        System.setProperty("java.security.krb5.conf", errorKrb5file.getAbsolutePath());
        Class classRef;
        if (System.getProperty("java.vendor").contains("IBM")) {
            classRef = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            classRef = Class.forName("sun.security.krb5.Config");
        }
        Method refreshMethod = classRef.getMethod("refresh");
        refreshMethod.invoke(classRef);
        KerberosName kerberosName = new KerberosName(TEST_PRINCIPAL);

        assertNotEquals(errorRealm, kerberosName.getDefaultRealm());

        KerberosUtil.refreshConfig();

        assertEquals(errorRealm, kerberosName.getDefaultRealm());

        System.setProperty("java.security.krb5.conf", kdc.getKrb5conf().getAbsolutePath());
        if (System.getProperty("java.vendor").contains("IBM")) {
            classRef = Class.forName("com.ibm.security.krb5.internal.Config");
        } else {
            classRef = Class.forName("sun.security.krb5.Config");
        }
        refreshMethod.invoke(classRef);
        KerberosUtil.refreshConfig();

        assertEquals("CHUNJUN.COM", kerberosName.getDefaultRealm());
    }

    @Test
    public void testReloadKrb5conf()
            throws IOException, ClassNotFoundException, NoSuchMethodException,
                    InvocationTargetException, IllegalAccessException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        StringBuilder sb = new StringBuilder();
        InputStream is2 = cl.getResourceAsStream("minikdc-krb5.conf");
        BufferedReader r = null;

        try {
            r = new BufferedReader(new InputStreamReader(is2, Charsets.UTF_8));

            for (String line = r.readLine(); line != null; line = r.readLine()) {
                sb.append(line).append("{3}");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(r);
            IOUtils.closeQuietly(is2);
        }
        String errorRealm =
                "CHUNJUN1".toUpperCase(Locale.ENGLISH) + "." + "COM".toUpperCase(Locale.ENGLISH);
        File errorKrb5file = new File(kdcDir.toString() + "/errorKrb5.conf");
        FileUtils.writeStringToFile(
                errorKrb5file,
                MessageFormat.format(
                        sb.toString(),
                        errorRealm,
                        kdc.getHost(),
                        Integer.toString(kdc.getPort()),
                        System.getProperty("line.separator")));

        KerberosName kerberosName = new KerberosName(TEST_PRINCIPAL);

        assertNotEquals(errorRealm, kerberosName.getDefaultRealm());

        KerberosUtil.reloadKrb5conf(errorKrb5file.getPath());

        assertEquals(errorRealm, kerberosName.getDefaultRealm());

        KerberosUtil.reloadKrb5conf(kdc.getKrb5conf().getPath());

        assertEquals("CHUNJUN.COM", kerberosName.getDefaultRealm());
    }

    @Test
    public void testCreateDirIfNotExisted() {
        String newPath = kdcDir.getPath() + "/" + UUID.randomUUID() + ".txt";
        String created = KerberosUtil.createDir(newPath);
        File file = new File(created);
        assertTrue(file.isDirectory());
    }

    @Test
    public void testCreateDirIfExisted() {
        String path = kdcDir.getPath() + "/" + UUID.randomUUID() + ".txt";
        File file = new File(path);
        if (!file.exists()) {
            file.mkdir();
        }
        String created = KerberosUtil.createDir(path);
        File newFile = new File(created);
        assertTrue(newFile.isDirectory());
    }

    @Test
    @DisplayName("test login and get ugi when use kerberos by configuration")
    public void testLoginAndReturnUgiWithConfiguration() throws IOException {
        Configuration conf = new Configuration();
        UserGroupInformation ugi =
                KerberosUtil.loginAndReturnUgi(conf, TEST_PRINCIPAL, keytab.getAbsolutePath());
        assertEquals(TEST_PRINCIPAL, ugi.getUserName());
    }

    @Test
    @DisplayName("test login and get ugi when use kerberos by hadoopConfig")
    public void testLoginAndReturnUgiWithHadoopConfig() {
        Map<String, Object> hadoopConfig =
                new HashMap<>(
                        ImmutableMap.<String, Object>builder()
                                .put("principalFile", keytab.getAbsolutePath())
                                .put(KEY_USE_LOCAL_FILE, true)
                                .build());
        UserGroupInformation ugi = KerberosUtil.loginAndReturnUgi(hadoopConfig, null, null);
        assertEquals(TEST_PRINCIPAL, ugi.getUserName());
    }

    @Test
    @DisplayName("test login and get ugi when use kerberos")
    public void testLoginAndReturnUgi() throws IOException {
        String keytabPath = keytab.getPath();
        String krb5Conf = kdc.getKrb5conf().getPath();
        UserGroupInformation ugi =
                KerberosUtil.loginAndReturnUgi(TEST_PRINCIPAL, keytabPath, krb5Conf);
        assertEquals(TEST_PRINCIPAL, ugi.getUserName());
    }

    @Test
    @DisplayName("test login and get ugi when use kerberos by kerberosConfig")
    public void testLoginAndReturnUgiWithKerberosConfig() throws IOException {
        String keytabPath = keytab.getPath();
        String krb5Conf = kdc.getKrb5conf().getPath();
        KerberosConfig kerberosConfig = new KerberosConfig(TEST_PRINCIPAL, keytabPath, krb5Conf);
        UserGroupInformation ugi = KerberosUtil.loginAndReturnUgi(kerberosConfig);
        assertEquals(TEST_PRINCIPAL, ugi.getUserName());
    }

    /**
     * kerberosConfig { "principalFile":"keytab.keytab", "remoteDir":"/home/admin", "sftpConf":{
     * "path" : "/home/admin", "password" : "******", "port" : "22", "auth" : "1", "host" :
     * "127.0.0.1", "username" : "admin" } }
     */
    @Test
    @DisplayName("test load file from local")
    public void testLoadFileFromLocal() {
        Map<String, Object> kerberosConfig = ImmutableMap.of(KEY_USE_LOCAL_FILE, true);
        String filePath = keytab.getPath();
        String localPath = KerberosUtil.loadFile(kerberosConfig, filePath, null, null);
        assertEquals(filePath, localPath);
    }

    @Test
    @DisplayName("test load file from blob server")
    public void testLoadFileFromBloBServer() {
        Map<String, Object> kerberosConfig = ImmutableMap.of(KEY_USE_LOCAL_FILE, false);
        String filePath = "/opt/blob_file";
        String localPath = KerberosUtil.loadFile(kerberosConfig, filePath, null, null);
        assertEquals(filePath, localPath);
    }

    @Test
    @DisplayName("test successful load file from distributedCache")
    public void testSuccessfulLoadFileFromDistributedCache() {
        Map<String, Object> kerberosConfig = ImmutableMap.of(KEY_USE_LOCAL_FILE, false);
        String filePath = "/opt/test_file";
        Map<String, Future<Path>> cacheCopyTasks =
                ImmutableMap.<String, Future<Path>>builder()
                        .put("test_file", Futures.immediateFuture(new Path(filePath)))
                        .build();
        DistributedCache distributedCache = new DistributedCache(cacheCopyTasks);
        String localPath =
                KerberosUtil.loadFile(kerberosConfig, filePath, distributedCache, null, null);
        assertEquals(filePath, localPath);
    }

    @Test
    @DisplayName("if fail to load file from distributedCache, it should load from sftp")
    public void testFailureLoadFileFromDistributedCache() {
        Map<String, Object> kerberosConfig =
                ImmutableMap.<String, Object>builder()
                        .put(KEY_USE_LOCAL_FILE, false)
                        .put(KerberosUtil.KEY_REMOTE_DIR, "")
                        .build();
        String filePath = "/opt/test_file";
        Map<String, Future<Path>> cacheCopyTasks =
                ImmutableMap.<String, Future<Path>>builder()
                        .put(
                                "test_file",
                                Futures.immediateFailedFuture(
                                        new IOException("this file is not found")))
                        .build();
        DistributedCache distributedCache = new DistributedCache(cacheCopyTasks);
        ChunJunRuntimeException thrown =
                assertThrows(
                        ChunJunRuntimeException.class,
                        () ->
                                KerberosUtil.loadFile(
                                        kerberosConfig, filePath, distributedCache, null, null),
                        "It should load from sftp and throw a ChunJunRuntimeException when remote dir is empty");
        assertTrue(thrown.getMessage().contains("can't find [remoteDir] in config"));
    }

    @Test
    public void testFileExistWhenFileIsNotExist() {
        assertFalse(KerberosUtil.fileExists("/0xCOFFEEBABY"));
    }

    @Test
    public void testFileExistWhenFileIsExistButIsDirectory() {
        String directory = keytab.getParent();
        assertFalse(KerberosUtil.fileExists(directory));
    }

    @Test
    public void testFileExistWhenFileIsExistAndIsFile() {
        String file = keytab.getPath();
        assertTrue(KerberosUtil.fileExists(file));
    }

    @Test
    public void testCheckFileExistWhenFileIsNotExist() {
        RuntimeException thrown =
                assertThrows(
                        RuntimeException.class,
                        () -> KerberosUtil.checkFileExists("/0xCOFFEEBABY"),
                        "It should load from sftp and throw a ChunJunRuntimeException when remote dir is empty");
        assertTrue(thrown.getMessage().contains("keytab file not exists"));
    }

    @Test
    public void testCheckFileExistWhenFileIsExistButIsDirectory() {
        String directory = keytab.getParent();
        RuntimeException thrown =
                assertThrows(
                        RuntimeException.class,
                        () -> KerberosUtil.checkFileExists(directory),
                        "It should load from sftp and throw a ChunJunRuntimeException when remote dir is empty");
        assertTrue(thrown.getMessage().contains("keytab is a directory"));
    }

    @Test
    public void testCheckFileExistWhenFileIsExistAndIsFile() {
        String file = keytab.getPath();
        assertDoesNotThrow(() -> KerberosUtil.checkFileExists(file));
    }

    @Test
    public void testGetPrincipalFromConfig() {
        Map<String, Object> configMap = ImmutableMap.of(KerberosUtil.KEY_PRINCIPAL, TEST_PRINCIPAL);
        String principal = KerberosUtil.getPrincipal(configMap, null);
        assertEquals(TEST_PRINCIPAL, principal);
    }

    @Test
    public void testGetPrincipalFromKeytab() {
        Map<String, Object> configMap = ImmutableMap.of();
        String path = keytab.getPath();
        String principal = KerberosUtil.getPrincipal(configMap, path);
        assertEquals(TEST_PRINCIPAL, principal);
    }

    @Test
    public void testFindPrincipalFromKeytab() {
        String path = keytab.getPath();
        String principal = KerberosUtil.findPrincipalFromKeytab(path);
        assertEquals(TEST_PRINCIPAL, principal);
    }

    @Test
    public void testSuccessfulGetPrincipalFileName() {
        String testPrincipalFile = "testPrincipalFile";
        Map<String, Object> config = ImmutableMap.of("principalFile", testPrincipalFile);
        String principalFile = KerberosUtil.getPrincipalFileName(config);
        assertEquals(testPrincipalFile, principalFile);
    }

    @Test
    public void testFailureGetPrincipalFileName() {
        Map<String, Object> config = ImmutableMap.of();
        RuntimeException thrown =
                assertThrows(
                        RuntimeException.class,
                        () -> KerberosUtil.getPrincipalFileName(config),
                        "It should throw a RuntimeException when get principal file but config is not contains");
        assertTrue(thrown.getMessage().contains("[principalFile]必须指定"));
    }
}
