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


package com.dtstack.flinkx.classloader;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.dtstack.flink.api.java.MyLocalStreamEnvironment;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.enums.OperatorType;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @author jiangbo
 * @date 2019/10/21
 */
public class PluginUtil {

    public static final String READER_SUFFIX = "reader";
    public static final String SOURCE_SUFFIX = "source";
    public static final String WRITER_SUFFIX = "writer";
    public static final String SINK_SUFFIX = "sink";

    private static final String PACKAGE_PREFIX = "com.dtstack.flinkx.connector.";

    private static final String JAR_PREFIX = "flinkx";

    private static final String FILE_PREFIX = "file:";

    private static final String SP = File.separator;

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";

    /**
     * 根据插件名称查找插件路径
     * @param pluginName 插件名称，如: kafkareader、kafkasource等
     * @param pluginRoot
     * @param remotePluginPath
     * @return
     */
    public static Set<URL> getJarFileDirPath(String pluginName, String pluginRoot, String remotePluginPath) {
        Set<URL> urlList = new HashSet<>();

        String pluginPath = Objects.isNull(remotePluginPath) ? pluginRoot : remotePluginPath;
        String name = pluginName.replace(READER_SUFFIX, "")
                .replace(SOURCE_SUFFIX, "")
                .replace(WRITER_SUFFIX, "")
                .replace(SINK_SUFFIX, "");

        try {
            String pluginJarPath = pluginRoot + SP + name;
            // 获取jar包名字，构建对应的URL地址
            for (String jarName : getJarNames(new File(pluginJarPath))) {
                urlList.add(new URL(FILE_PREFIX + pluginPath + SP + name + SP + jarName));
            }
            return urlList;
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> getJarNames(File pluginPath) {
        List<String> jarNames = new ArrayList<>();
        if (pluginPath.exists() && pluginPath.isDirectory()) {
            File[] jarFiles = pluginPath.listFiles((dir, name) ->
                    name.toLowerCase().startsWith(JAR_PREFIX) && name.toLowerCase().endsWith(".jar"));

            if (Objects.nonNull(jarFiles) && jarFiles.length > 0) {
                Arrays.stream(jarFiles).forEach(item -> jarNames.add(item.getName()));
            }
        }
        return jarNames;
    }

    /**
     * 根据插件名称查找插件入口类
     * @param pluginName 如：kafkareader
     * @param operatorType 算子类型
     * @return
     */
    public static String getPluginClassName(String pluginName, OperatorType operatorType) {
        String pluginClassName;
        switch (operatorType){
            case source:
                String sourceName = pluginName.replace(READER_SUFFIX, SOURCE_SUFFIX);
                pluginClassName = PACKAGE_PREFIX + camelize(sourceName, SOURCE_SUFFIX);
                break;
            case sink:
                String sinkName = pluginName.replace(WRITER_SUFFIX, SINK_SUFFIX);
                pluginClassName = PACKAGE_PREFIX + camelize(sinkName, SINK_SUFFIX);
                break;
            default:
                throw new IllegalArgumentException("Plugin Name should end with reader, writer, current plugin name is: " + pluginName);
        }

        return pluginClassName;
    }

    private static String camelize(String pluginName, String suffix) {
        int pos = pluginName.indexOf(suffix);
        String left = pluginName.substring(0, pos);
        left = left.toLowerCase();
        suffix = suffix.toLowerCase();
        StringBuffer sb = new StringBuffer();
        sb.append(left).append(".").append(suffix).append(".");
        sb.append(left.substring(0, 1).toUpperCase()).append(left.substring(1));
        sb.append(suffix.substring(0, 1).toUpperCase()).append(suffix.substring(1));
        return sb.toString();
    }

    public static void registerPluginUrlToCachedFile(SyncConf config, StreamExecutionEnvironment env) {
        Set<URL> urlSet = new HashSet<>();

        Set<URL> sourceUrlList = PluginUtil.getJarFileDirPath(config.getReader().getName(), config.getPluginRoot(), config.getRemotePluginPath());
        Set<URL> sinkUrlList = PluginUtil.getJarFileDirPath(config.getWriter().getName(), config.getPluginRoot(), config.getRemotePluginPath());

        urlSet.addAll(sourceUrlList);
        urlSet.addAll(sinkUrlList);

        int i = 0;
        for (URL url : urlSet) {
            String classFileName = String.format(CLASS_FILE_NAME_FMT, i);
            env.registerCachedFile(url.getPath(), classFileName, true);
            i++;
        }

        if (env instanceof MyLocalStreamEnvironment) {
            ((MyLocalStreamEnvironment) env).setClasspaths(new ArrayList<>(urlSet));
        }
    }
}
