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

package com.dtstack.chunjun.util;

import com.dtstack.chunjun.config.ContentConfig;
import com.dtstack.chunjun.config.JobConfigBuilder;
import com.dtstack.chunjun.config.OperatorConfig;
import com.dtstack.chunjun.config.SettingConfigBuilder;
import com.dtstack.chunjun.config.SpeedConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.config.SyncConfigBuilder;
import com.dtstack.chunjun.constants.ConfigConstant;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.enums.ClusterMode;
import com.dtstack.chunjun.options.Options;
import com.dtstack.chunjun.source.SourceFactoryTest;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.dtstack.chunjun.source.DtInputFormatSourceFunctionTest.classloaderSafeInvoke;
import static com.dtstack.chunjun.util.PluginUtil.getJarFileDirPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PluginUtilTest {

    @TempDir static File localPluginRoot;

    static File testJar;

    @BeforeAll
    public static void beforeAll() throws IOException {
        testJar = new File(localPluginRoot.getPath() + "/chunjun-test.jar");
        testJar.createNewFile();
    }

    @Test
    public void testGetJarFileDirPath() throws IOException {
        String pluginName = "hbasereader";
        String pluginRoot = localPluginRoot.getPath();
        String remotePluginPath = "/opt/remote";
        String suffix = "test";
        File suffixDir = new File(pluginRoot + "/" + suffix);
        suffixDir.mkdirs();
        String pluginPath = suffixDir.getPath() + "/hbase14";
        File pluginDir = new File(pluginPath);
        pluginDir.mkdirs();
        File testJar = new File(pluginDir + "/chunjun-test.jar");
        testJar.createNewFile();
        Set<URL> urlSet = getJarFileDirPath(pluginName, pluginRoot, remotePluginPath, suffix);
        assertTrue(urlSet.contains(testJar.toURL()));
    }

    @Test
    public void testCamelize() {
        String camelizeName = PluginUtil.camelize("binlogsource", "source");
        assertEquals(
                "com.dtstack.chunjun.connector.binlog.source.BinlogSourceFactory", camelizeName);
    }

    @Test
    public void testCreateDistributedCacheFromContextClassLoader() {
        classloaderSafeInvoke(
                () -> {
                    ImmutableList<URL> uriList = ImmutableList.of(testJar.toURL());
                    URL[] uris = uriList.toArray(new URL[0]);
                    URLClassLoader urlClassLoader =
                            new URLClassLoader(uris, this.getClass().getClassLoader());
                    Thread.currentThread().setContextClassLoader(urlClassLoader);
                    DistributedCache distributedCache =
                            PluginUtil.createDistributedCacheFromContextClassLoader();
                    File file = distributedCache.getFile("chunjun-test.jar");
                    assertNotNull(file);
                    assertEquals(testJar.getPath(), file.getPath());
                });
    }

    @Test
    public void testSetPipelineOptionsToEnvConfig() {
        StreamExecutionEnvironment environment =
                new SourceFactoryTest.DummyStreamExecutionEnvironment();
        ImmutableList<String> uriList = ImmutableList.of(testJar.getPath());
        List<String> pipelineJars =
                PluginUtil.setPipelineOptionsToEnvConfig(
                        environment, uriList, ClusterMode.local.name());
        System.out.println(pipelineJars);
        assertFalse(pipelineJars.isEmpty());
        assertTrue(pipelineJars.contains(testJar.getPath()));
    }

    @Test
    public void testRegisterShipfileToCachedFile() {
        SourceFactoryTest.DummyStreamExecutionEnvironment environment =
                new SourceFactoryTest.DummyStreamExecutionEnvironment();
        String shipFileStr = testJar.getPath();
        PluginUtil.registerShipfileToCachedFile(shipFileStr, environment);
        Map<String, String> cachedFileMap = environment.getCachedFileMap();

        assertTrue(cachedFileMap.containsKey("chunjun-test.jar"));
        assertTrue(cachedFileMap.containsValue(shipFileStr));
    }

    @Test
    public void testRegisterPluginUrlToCachedFile() throws IOException {
        Options options = new Options();
        SpeedConfig speedConfig = new SpeedConfig();
        speedConfig.setChannel(5);
        speedConfig.setWriterChannel(3);

        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig reader = new OperatorConfig();
        reader.setName("hbasereader");
        reader.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(ConfigConstant.KEY_COLUMN, ImmutableList.of(ConstantValue.STAR_SYMBOL))
                        .build());
        contentConfig.setReader(reader);
        OperatorConfig writer = new OperatorConfig();
        writer.setName("hbasewriter");
        writer.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(ConfigConstant.KEY_COLUMN, ImmutableList.of(ConstantValue.STAR_SYMBOL))
                        .build());
        contentConfig.setWriter(writer);
        SyncConfig syncConfig =
                SyncConfigBuilder.newBuilder()
                        .job(
                                JobConfigBuilder.newBuilder()
                                        .setting(
                                                SettingConfigBuilder.newBuilder()
                                                        .speed(speedConfig)
                                                        .build())
                                        .content(new LinkedList<>(ImmutableList.of(contentConfig)))
                                        .build())
                        .pluginRoot(localPluginRoot.getPath())
                        .build();
        StreamExecutionEnvironment environment =
                new SourceFactoryTest.DummyStreamExecutionEnvironment();

        PluginUtil.registerPluginUrlToCachedFile(options, syncConfig, environment);

        List<String> jarList = syncConfig.getSyncJarList();

        assertTrue(jarList.contains(testJar.getPath()));
    }
}
