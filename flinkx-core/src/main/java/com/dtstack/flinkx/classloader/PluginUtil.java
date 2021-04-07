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

import com.dtstack.flink.api.java.MyLocalStreamEnvironment;
import com.dtstack.flinkx.conf.FlinkxConf;
import com.dtstack.flinkx.enums.OperatorType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

    private static final String READER_SUFFIX = "reader";
    private static final String SOURCE_SUFFIX = "source";
    private static final String WRITER_SUFFIX = "writer";
    private static final String SINK_SUFFIX = "sink";

    private static final String PACKAGE_PREFIX = "com.dtstack.flinkx.connectors.";

    private static final String JAR_PREFIX = "flinkx";

    private static final String FILE_PREFIX = "file:";

    private static final String SP = File.separator;

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";

    /**
     * 根据插件名称查找插件路径
     * @param pluginName kafka、stream等
     * @param pluginRoot
     * @param remotePluginPath
     * @return
     */
    public static Set<URL> getJarFileDirPath(String pluginName, String pluginRoot, String remotePluginPath) {
        Set<URL> urlList = new HashSet<>();

        String pluginPath = Objects.isNull(remotePluginPath) ? pluginRoot : remotePluginPath;

        try {
            String pluginJarPath = pluginRoot + SP + pluginName;
            // 获取jar包名字，构建对应的URL地址
            for (String jarName : getJarNames(new File(pluginJarPath))) {
                urlList.add(new URL(FILE_PREFIX + pluginPath + SP + pluginName + SP + jarName));
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

    public static void registerPluginUrlToCachedFile(FlinkxConf config, StreamExecutionEnvironment env) {
        String readerPluginName = config.getReader().getName().replace(READER_SUFFIX, "").replace(SOURCE_SUFFIX, "");
        Set<URL> readerUrlList = PluginUtil.getJarFileDirPath(readerPluginName, config.getPluginRoot(), config.getRemotePluginPath());

        String writerPluginName = config.getWriter().getName().replace(WRITER_SUFFIX, "").replace(SINK_SUFFIX, "");
        Set<URL> writerUrlList = PluginUtil.getJarFileDirPath(writerPluginName, config.getPluginRoot(), config.getRemotePluginPath());

        Set<URL> urlSet = new HashSet<>();

        urlSet.addAll(readerUrlList);
        urlSet.addAll(writerUrlList);

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
