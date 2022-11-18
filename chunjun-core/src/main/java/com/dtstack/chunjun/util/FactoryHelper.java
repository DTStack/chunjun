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
package com.dtstack.chunjun.util;

import com.dtstack.chunjun.constants.ConstantValue;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Slf4j
public class FactoryHelper {
    /** shipfile需要的jar的classPath name */
    public static final ConfigOption<String> CLASS_FILE_NAME_FMT =
            ConfigOptions.key("class_file_name_fmt")
                    .stringType()
                    .defaultValue("class_path_%d")
                    .withDescription("");

    /** 插件路径 */
    protected String localPluginPath = null;
    /** 远端插件路径 */
    protected String remotePluginPath = null;
    /** 插件加载类型 */
    protected String pluginLoadMode = ConstantValue.SHIP_FILE_PLUGIN_LOAD_MODE;
    /** 上下文环境 */
    protected StreamExecutionEnvironment env = null;
    /** shipfile需要的jar */
    protected List<URL> classPathSet = new ArrayList<>();
    /** shipfile需要的jar的classPath index */
    protected int classFileNameIndex = 0;
    /** 任务执行模式 */
    protected String executionMode;

    public FactoryHelper() {}

    /**
     * register plugin jar file
     *
     * @param factoryIdentifier
     * @param classLoader
     * @param dirName
     */
    public void registerCachedFile(
            String factoryIdentifier, ClassLoader classLoader, String dirName) {
        Set<URL> urlSet =
                PluginUtil.getJarFileDirPath(
                        factoryIdentifier, this.localPluginPath, this.remotePluginPath, dirName);
        try {
            Method add = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            add.setAccessible(true);
            List<String> urlList = new ArrayList<>(urlSet.size());
            for (URL jarUrl : urlSet) {
                add.invoke(classLoader, jarUrl);
                if (!this.classPathSet.contains(jarUrl)) {
                    urlList.add(jarUrl.toString());
                    this.classPathSet.add(jarUrl);
                    String classFileName =
                            String.format(
                                    CLASS_FILE_NAME_FMT.defaultValue(), this.classFileNameIndex);
                    this.env.registerCachedFile(jarUrl.getPath(), classFileName, true);
                    this.classFileNameIndex++;
                }
            }
            PluginUtil.setPipelineOptionsToEnvConfig(this.env, urlList, executionMode);
        } catch (Exception e) {
            log.warn("can't add jar in {} to cachedFile, e = {}", urlSet, e.getMessage());
        }
    }

    public void setLocalPluginPath(String localPluginPath) {
        this.localPluginPath = localPluginPath;
    }

    public void setRemotePluginPath(String remotePluginPath) {
        this.remotePluginPath = remotePluginPath;
    }

    public void setPluginLoadMode(String pluginLoadMode) {
        this.pluginLoadMode = pluginLoadMode;
    }

    public void setEnv(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public String getExecutionMode() {
        return executionMode;
    }

    public void setExecutionMode(String executionMode) {
        this.executionMode = executionMode;
    }
}
