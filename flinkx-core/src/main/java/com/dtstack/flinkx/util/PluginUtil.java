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


package com.dtstack.flinkx.util;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.factories.FactoryUtil;

import com.dtstack.flinkx.conf.FlinkXConf;
import com.dtstack.flinkx.enums.OperatorType;
import com.dtstack.flinkx.environment.MyLocalStreamEnvironment;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Reason:
 * Date: 2018/6/27
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class PluginUtil {
    public static final String READER_SUFFIX = "reader";
    private static final String SP = File.separator;

    private static final String JAR_SUFFIX = ".jar";
    public static final String SOURCE_SUFFIX = "source";
    public static final String WRITER_SUFFIX = "writer";
    public static final String SINK_SUFFIX = "sink";
    private static final Logger LOG = LoggerFactory.getLogger(PluginUtil.class);
    private static final String PACKAGE_PREFIX = "com.dtstack.flinkx.connector.";

    private static final String JAR_PREFIX = "flinkx";

    private static final String FILE_PREFIX = "file:";

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";

    public static String getJarFileDirPath(String type, String sqlRootDir) {
        String jarPath = sqlRootDir + SP + type;

        checkJarFileDirPath(sqlRootDir, jarPath);

        return jarPath;
    }

    private static void checkJarFileDirPath(String sqlRootDir, String path) {
        if (sqlRootDir == null || sqlRootDir.isEmpty()) {
            throw new RuntimeException("sqlPlugin is empty !");
        }

        File jarFile = new File(path);

        if (!jarFile.exists()) {
            throw new RuntimeException(String.format("path %s not exists!!!", path));
        }
    }

    public static URL[] getPluginJarUrls(String pluginDir, String factoryIdentifier) throws MalformedURLException {
        List<URL> urlList = new ArrayList<>();

        File dirFile = new File(pluginDir);

        if (pluginDir.contains("null")) {
            return urlList.toArray(new URL[0]);
        }

        if (!dirFile.exists() || !dirFile.isDirectory()) {
            throw new RuntimeException("plugin path:" + pluginDir + "is not exist.");
        }

        File[] files = dirFile.listFiles(tmpFile -> tmpFile.isFile() && tmpFile.getName().endsWith(JAR_SUFFIX));
        if (files == null || files.length == 0) {
            throw new RuntimeException("plugin path:" + pluginDir + " is null.");
        }

        for (File file : files) {
            URL pluginJarUrl = file.toURI().toURL();
            // format只加载一个jar
            if (pluginDir.endsWith(FactoryUtil.FORMATS.key())) {
                if (file.getName().contains(factoryIdentifier)) {
                    urlList.add(pluginJarUrl);
                }
            } else {
                urlList.add(pluginJarUrl);
            }
        }

        if (urlList == null || urlList.size() == 0) {
            throw new RuntimeException("no match jar in :" + pluginDir + " directory ，factoryIdentifier is :" + factoryIdentifier);
        }

        return urlList.toArray(new URL[0]);
    }

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

    public static void registerPluginUrlToCachedFile(FlinkXConf config, StreamExecutionEnvironment env) {
        Set<URL> urlSet = new HashSet<>();
        Set<URL> coreUrlList = getJarFileDirPath("", config.getPluginRoot(), config.getRemotePluginPath());
        Set<URL> sourceUrlList = getJarFileDirPath(config.getReader().getName(), config.getPluginRoot(), config.getRemotePluginPath());
        Set<URL> sinkUrlList = getJarFileDirPath(config.getWriter().getName(), config.getPluginRoot(), config.getRemotePluginPath());

        urlSet.addAll(coreUrlList);
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
            if(CollectionUtils.isNotEmpty(coreUrlList)){
                try {
                    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                    Method add = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                    add.setAccessible(true);
                    add.invoke(contextClassLoader, new ArrayList<>(coreUrlList).get(0));
                }catch (Exception e){
                    LOG.warn("cannot add core jar into contextClassLoader, coreUrlList = {}", GsonUtil.GSON.toJson(coreUrlList), e);
                }
            }
        }
    }
}
