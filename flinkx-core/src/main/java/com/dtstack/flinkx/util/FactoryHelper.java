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
package com.dtstack.flinkx.util;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2021/08/05 Company: www.dtstack.com
 *
 * @author tudou
 */
public class FactoryHelper {
    /** shipfile需要的jar的classPath name */
    public static final ConfigOption<String> CLASS_FILE_NAME_FMT =
            ConfigOptions.key("class_file_name_fmt")
                    .stringType()
                    .defaultValue("class_path_%d")
                    .withDescription("");

    private static final Logger LOG = LoggerFactory.getLogger(FactoryHelper.class);
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

    public FactoryHelper() {}

    /**
     * register plugin jar file
     *
     * @param factoryIdentifier
     * @param classLoader
     * @param ignore
     */
    public void registerCachedFile(
            String factoryIdentifier, ClassLoader classLoader, boolean ignore) {
        String pluginPath =
                StringUtils.equalsIgnoreCase(
                                this.pluginLoadMode, ConstantValue.CLASS_PATH_PLUGIN_LOAD_MODE)
                        ? this.remotePluginPath
                        : this.localPluginPath;
        String pluginJarPath =
                pluginPath
                        + File.separatorChar
                        + ConstantValue.CONNECTOR_DIR_NAME
                        + File.separatorChar
                        + factoryIdentifier;
        try {
            File pluginJarPathFile = new File(pluginJarPath);
            // 路径不存在或者不为文件夹
            if (!pluginJarPathFile.exists() || !pluginJarPathFile.isDirectory()) {
                if (ignore) {
                    return;
                } else {
                    throw new FlinkxRuntimeException(
                            "plugin path:" + pluginJarPath + " is not exist.");
                }
            }

            File[] files =
                    pluginJarPathFile.listFiles(
                            tmpFile -> tmpFile.isFile() && tmpFile.getName().endsWith(".jar"));
            if (files == null || files.length == 0) {
                throw new FlinkxRuntimeException("plugin path:" + pluginJarPath + " is null.");
            }

            Method add = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            add.setAccessible(true);

            for (File file : files) {
                URL jarUrl = file.toURI().toURL();
                add.invoke(classLoader, jarUrl);
                if (!this.classPathSet.contains(jarUrl)) {
                    this.classPathSet.add(jarUrl);
                    String classFileName =
                            String.format(
                                    CLASS_FILE_NAME_FMT.defaultValue(), this.classFileNameIndex);
                    this.env.registerCachedFile(jarUrl.getPath(), classFileName, true);
                    this.classFileNameIndex++;
                }
            }
        } catch (Exception e) {
            LOG.warn("can't add jar in {} to cachedFile, e = {}", pluginJarPath, e.getMessage());
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
}
