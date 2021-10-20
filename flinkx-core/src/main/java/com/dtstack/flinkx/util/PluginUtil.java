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

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.dirty.DirtyConf;
import com.dtstack.flinkx.dirty.utils.DirtyConfUtil;
import com.dtstack.flinkx.enums.OperatorType;
import com.dtstack.flinkx.environment.MyLocalStreamEnvironment;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static com.dtstack.flinkx.constants.ConstantValue.CONNECTOR_DIR_NAME;
import static com.dtstack.flinkx.constants.ConstantValue.DIRTY_DATA_DIR_NAME;
import static com.dtstack.flinkx.constants.ConstantValue.POINT_SYMBOL;

/**
 * Reason: Date: 2018/6/27 Company: www.dtstack.com
 *
 * @author xuchao
 */
public class PluginUtil {
    public static final String FORMATS_SUFFIX = "formats";
    public static final String DIRTY_SUFFIX = "dirty-data-collector";
    public static final String READER_SUFFIX = "reader";
    private static final String JAR_SUFFIX = ".jar";
    public static final String SOURCE_SUFFIX = "source";
    public static final String WRITER_SUFFIX = "writer";
    public static final String SINK_SUFFIX = "sink";
    public static final String GENERIC_SUFFIX = "Factory";
    public static final String METRIC_SUFFIX = "metrics";
    public static final String DEFAULT_METRIC_PLUGIN = "prometheus";
    private static final String SP = File.separator;
    private static final Logger LOG = LoggerFactory.getLogger(PluginUtil.class);
    private static final String PACKAGE_PREFIX = "com.dtstack.flinkx.connector.";
    private static final String METRIC_PACKAGE_PREFIX = "com.dtstack.flinkx.metrics.";
    private static final String METRIC_REPORT_PREFIX = "Report";
    private static final String DIRTY_PACKAGE_STR = "com.dtstack.flinkx.dirty.";
    private static final String DIRTY_CLASS_SUFFIX = "DirtyDataCollector";

    private static final String JAR_PREFIX = "flinkx";

    private static final String FILE_PREFIX = "file:";

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";

    /**
     * 根据插件名称查找插件路径
     *
     * @param pluginName 插件名称，如: kafkareader、kafkasource等
     * @param pluginRoot
     * @param remotePluginPath
     * @return
     */
    public static Set<URL> getJarFileDirPath(
            String pluginName, String pluginRoot, String remotePluginPath) {
        Set<URL> urlList = new HashSet<>();

        String pluginPath = Objects.isNull(remotePluginPath) ? pluginRoot : remotePluginPath;
        String name =
                pluginName
                        .replace(READER_SUFFIX, "")
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

    /**
     * 获取路径当前层级下所有jar包名称
     *
     * @param pluginPath
     * @return
     */
    private static List<String> getJarNames(File pluginPath) {
        List<String> jarNames = new ArrayList<>();
        if (pluginPath.exists() && pluginPath.isDirectory()) {
            File[] jarFiles =
                    pluginPath.listFiles(
                            (dir, name) ->
                                    name.toLowerCase().startsWith(JAR_PREFIX)
                                            && name.toLowerCase().endsWith(".jar"));

            if (Objects.nonNull(jarFiles) && jarFiles.length > 0) {
                Arrays.stream(jarFiles).forEach(item -> jarNames.add(item.getName()));
            }
        }
        return jarNames;
    }

    /**
     * 根据插件名称查找插件入口类
     *
     * @param pluginName 如：kafkareader
     * @param operatorType 算子类型
     * @return
     */
    public static String getPluginClassName(String pluginName, OperatorType operatorType) {
        String pluginClassName;
        switch (operatorType) {
            case metric:
                pluginClassName = appendMetricClass(pluginName);
                break;
            case source:
                String sourceName = pluginName.replace(READER_SUFFIX, SOURCE_SUFFIX);
                pluginClassName = camelize(sourceName, SOURCE_SUFFIX);
                break;
            case sink:
                String sinkName = pluginName.replace(WRITER_SUFFIX, SINK_SUFFIX);
                pluginClassName = camelize(sinkName, SINK_SUFFIX);
                break;
            case dirty:
                pluginClassName =
                        DIRTY_PACKAGE_STR
                                + pluginName
                                + "."
                                + DtStringUtil.captureFirstLetter(pluginName)
                                + DIRTY_CLASS_SUFFIX;
                break;
            default:
                throw new FlinkxRuntimeException("unknown operatorType: " + operatorType);
        }

        return pluginClassName;
    }

    /**
     * 拼接插件包类全限定名
     *
     * @param pluginName 插件包名称，如：binlogsource
     * @param suffix 插件类型前缀，如：source、sink
     * @return 插件包类全限定名，如：com.dtstack.flinkx.connector.binlog.source.BinlogSourceFactory
     */
    private static String camelize(String pluginName, String suffix) {
        int pos = pluginName.indexOf(suffix);
        String left = pluginName.substring(0, pos);
        left = left.toLowerCase();
        suffix = suffix.toLowerCase();
        StringBuilder sb = new StringBuilder(32);
        sb.append(PACKAGE_PREFIX);
        sb.append(ConnectorNameConvertUtil.convertPackageName(left))
                .append(ConstantValue.POINT_SYMBOL)
                .append(suffix)
                .append(ConstantValue.POINT_SYMBOL);
        left = ConnectorNameConvertUtil.convertClassPrefix(left);
        sb.append(left.substring(0, 1).toUpperCase()).append(left.substring(1));
        sb.append(suffix.substring(0, 1).toUpperCase()).append(suffix.substring(1));
        sb.append(GENERIC_SUFFIX);
        return sb.toString();
    }

    private static String appendMetricClass(String pluginName) {
        StringBuilder sb = new StringBuilder(32);
        sb.append(METRIC_PACKAGE_PREFIX)
                .append(pluginName.toLowerCase(Locale.ENGLISH))
                .append(POINT_SYMBOL);
        sb.append(pluginName.substring(0, 1).toUpperCase())
                .append(pluginName.substring(1).toLowerCase());
        sb.append(METRIC_REPORT_PREFIX);
        return sb.toString();
    }

    /**
     * 将任务所用到的插件包注册到env中
     *
     * @param config
     * @param env
     */
    public static void registerPluginUrlToCachedFile(
            Options options, SyncConf config, StreamExecutionEnvironment env) {
        DirtyConf dirtyConf = DirtyConfUtil.parse(options);
        Set<URL> urlSet = new HashSet<>();
        Set<URL> coreUrlList = getJarFileDirPath("", config.getPluginRoot(), null);
        Set<URL> formatsUrlList = getJarFileDirPath(FORMATS_SUFFIX, config.getPluginRoot(), null);
        Set<URL> sourceUrlList =
                getJarFileDirPath(
                        config.getReader().getName(),
                        config.getPluginRoot() + SP + CONNECTOR_DIR_NAME,
                        null);
        Set<URL> sinkUrlList =
                getJarFileDirPath(
                        config.getWriter().getName(),
                        config.getPluginRoot() + SP + CONNECTOR_DIR_NAME,
                        null);
        Set<URL> metricUrlList =
                getJarFileDirPath(
                        config.getMetricPluginConf().getPluginName(),
                        config.getPluginRoot() + SP + METRIC_SUFFIX,
                        null);

        Set<URL> dirtyUrlList =
                getJarFileDirPath(
                        dirtyConf.getType(),
                        config.getPluginRoot() + SP + DIRTY_DATA_DIR_NAME,
                        null);

        urlSet.addAll(coreUrlList);
        urlSet.addAll(formatsUrlList);
        urlSet.addAll(sourceUrlList);
        urlSet.addAll(sinkUrlList);
        urlSet.addAll(metricUrlList);
        urlSet.addAll(dirtyUrlList);

        int i = 0;
        for (URL url : urlSet) {
            String classFileName = String.format(CLASS_FILE_NAME_FMT, i);
            env.registerCachedFile(url.getPath(), classFileName, true);
            i++;
        }

        if (env instanceof MyLocalStreamEnvironment) {
            ((MyLocalStreamEnvironment) env).setClasspaths(new ArrayList<>(urlSet));
            if (CollectionUtils.isNotEmpty(coreUrlList)) {
                try {
                    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                    Method add = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
                    add.setAccessible(true);
                    add.invoke(contextClassLoader, new ArrayList<>(coreUrlList).get(0));
                } catch (Exception e) {
                    LOG.warn(
                            "cannot add core jar into contextClassLoader, coreUrlList = {}",
                            GsonUtil.GSON.toJson(coreUrlList),
                            e);
                }
            }
        }
    }

    /**
     * register shipfile to StreamExecutionEnvironment cachedFile
     *
     * @param shipfile the shipfile which needed to add into cacheFile
     * @param env StreamExecutionEnvironment
     */
    public static void registerShipfileToCachedFile(
            String shipfile, StreamExecutionEnvironment env) {
        if (StringUtils.isNotBlank(shipfile)) {
            String[] files = shipfile.split(ConstantValue.COMMA_SYMBOL);
            Set<String> fileNameSet = new HashSet<>(8);
            for (String filePath : files) {
                String fileName = new File(filePath).getName();
                if (fileNameSet.contains(fileName)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "can not add duplicate fileName to cachedFiles, duplicate fileName = %s, shipfile = %s",
                                    fileName, shipfile));
                } else if (!new File(filePath).exists()) {
                    throw new IllegalArgumentException(
                            String.format("file: [%s] is not exists", filePath));
                } else {
                    env.registerCachedFile(filePath, fileName, false);
                    fileNameSet.add(fileName);
                }
            }
            fileNameSet.clear();
        }
    }

    /**
     * Create DistributedCache from the URL of the ContextClassLoader
     *
     * @return
     */
    public static DistributedCache createDistributedCacheFromContextClassLoader() {
        Map<String, Future<Path>> distributeCachedFiles = new HashMap<>();

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader instanceof URLClassLoader) {
            URLClassLoader classLoader = (URLClassLoader) contextClassLoader;
            URL[] urLs = classLoader.getURLs();

            for (URL url : urLs) {
                String path = url.getPath();
                String name =
                        path.substring(path.lastIndexOf(ConstantValue.SINGLE_SLASH_SYMBOL) + 1);
                distributeCachedFiles.put(
                        name,
                        CompletableFuture.completedFuture(new org.apache.flink.core.fs.Path(path)));
            }
            return new DistributedCache(distributeCachedFiles);
        } else {
            LOG.warn("ClassLoader: {} is not instanceof URLClassLoader", contextClassLoader);
            return null;
        }
    }
}
