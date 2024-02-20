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

import com.dtstack.chunjun.cdc.config.CacheConfig;
import com.dtstack.chunjun.cdc.config.DDLConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.dirty.utils.DirtyConfUtil;
import com.dtstack.chunjun.enums.ClusterMode;
import com.dtstack.chunjun.enums.OperatorType;
import com.dtstack.chunjun.options.Options;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

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
import java.util.stream.Collectors;

import static com.dtstack.chunjun.constants.ConstantValue.CONNECTOR_DIR_NAME;
import static com.dtstack.chunjun.constants.ConstantValue.DDL_CONVENT_DIR_NAME;
import static com.dtstack.chunjun.constants.ConstantValue.DIRTY_DATA_DIR_NAME;
import static com.dtstack.chunjun.constants.ConstantValue.POINT_SYMBOL;
import static com.dtstack.chunjun.constants.ConstantValue.RESTORE_DIR_NAME;

@Slf4j
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
    private static final String PACKAGE_PREFIX = "com.dtstack.chunjun.connector.";
    private static final String METRIC_PACKAGE_PREFIX = "com.dtstack.chunjun.metrics.";
    private static final String METRIC_REPORT_PREFIX = "Report";
    private static final String DIRTY_PACKAGE_STR = "com.dtstack.chunjun.dirty.";
    private static final String RESTORE_PACKAGE_STR = "com.dtstack.chunjun.restore.";
    private static final String RESTORE_DDL_CONVENT = "com.dtstack.chunjun.ddl.convent.";
    private static final String DIRTY_CLASS_SUFFIX = "DirtyDataCollector";
    private static final String DDL_HANDLER_CLASS_SUFFIX = "DDLHandler";
    private static final String CACHE_HANDLER_CLASS_SUFFIX = "CacheHandler";
    private static final String DDL_CONVENT_CLASS_SUFFIX = "DdlConventImpl";

    private static final String JAR_PREFIX = "chunjun";

    private static final String FILE_PREFIX = "file:";

    private static final String CLASS_FILE_NAME_FMT = "class_path_%d";

    /**
     * 根据插件名称查找插件路径
     *
     * @param pluginName 插件名称，如: kafkareader、kafkasource等
     * @param pluginRoot
     * @param remotePluginPath
     * @param suffix
     * @return
     */
    public static Set<URL> getJarFileDirPath(
            String pluginName, String pluginRoot, String remotePluginPath, String suffix) {
        Set<URL> urlSet = new HashSet<>();
        String name =
                pluginName
                        .replace(READER_SUFFIX, "")
                        .replace(SOURCE_SUFFIX, "")
                        .replace(WRITER_SUFFIX, "")
                        .replace(SINK_SUFFIX, "");
        name = ConnectorNameConvertUtil.convertPackageName(name);
        getJarUrlList(pluginRoot, suffix, name, urlSet);
        getJarUrlList(remotePluginPath, suffix, name, urlSet);

        return urlSet;
    }

    /**
     * 根据插件名称查找ddl解析插件路径
     *
     * @param pluginName 插件名称，如: kafkareader、kafkasource等
     * @param pluginRoot
     * @param remotePluginPath
     * @param suffix
     * @return
     */
    public static Set<URL> getDdlJarFileDirPath(
            String pluginName, String pluginRoot, String remotePluginPath, String suffix) {
        Set<URL> urlSet = new HashSet<>();
        String name =
                pluginName
                        .replace(READER_SUFFIX, "")
                        .replace(SOURCE_SUFFIX, "")
                        .replace(WRITER_SUFFIX, "")
                        .replace(SINK_SUFFIX, "");
        name = DdlConventNameConvertUtil.convertPackageName(name);
        getJarUrlList(pluginRoot, suffix, name, urlSet);
        getJarUrlList(remotePluginPath, suffix, name, urlSet);

        return urlSet;
    }

    /**
     * Obtain local and remote ChunJun plugin jar package path
     *
     * @param pluginPath
     * @param suffix
     * @param pluginName
     * @param urlSet
     * @return
     */
    private static Set<URL> getJarUrlList(
            String pluginPath, String suffix, String pluginName, Set<URL> urlSet) {
        if (StringUtils.isBlank(pluginPath)) {
            return urlSet;
        }
        try {
            String pluginPathDir;
            if (new File(pluginPath).exists()) {
                if (StringUtils.isNotBlank(suffix)) {
                    pluginPathDir = pluginPath + SP + suffix;
                } else {
                    pluginPathDir = pluginPath;
                }
                String pluginJarPath = pluginPathDir + SP + pluginName;
                // 获取jar包名字，构建对应的URL地址
                for (String jarName : getJarNames(new File(pluginJarPath))) {
                    urlSet.add(new File(pluginJarPath + SP + jarName).toURI().toURL());
                }
            }
        } catch (MalformedURLException e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "can't get chunjun jar from %s, suffix = %s, pluginName = %s",
                            pluginPath, suffix, pluginName),
                    e);
        }
        return urlSet;
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
                                + StringUtil.captureFirstLetter(pluginName)
                                + DIRTY_CLASS_SUFFIX;
                break;
            case ddl:
                pluginClassName =
                        RESTORE_PACKAGE_STR
                                + pluginName
                                + "."
                                + StringUtil.captureFirstLetter(pluginName)
                                + DDL_HANDLER_CLASS_SUFFIX;
                break;

            case cache:
                pluginClassName =
                        RESTORE_PACKAGE_STR
                                + pluginName
                                + "."
                                + StringUtil.captureFirstLetter(pluginName)
                                + CACHE_HANDLER_CLASS_SUFFIX;
                break;

            case ddlConvent:
                pluginClassName =
                        RESTORE_DDL_CONVENT
                                + pluginName
                                + "."
                                + StringUtil.captureFirstLetter(pluginName)
                                + DDL_CONVENT_CLASS_SUFFIX;
                break;

            default:
                throw new ChunJunRuntimeException("unknown operatorType: " + operatorType);
        }

        return pluginClassName;
    }

    /**
     * 拼接插件包类全限定名
     *
     * @param pluginName 插件包名称，如：binlogsource
     * @param suffix 插件类型前缀，如：source、sink
     * @return 插件包类全限定名，如：com.dtstack.chunjun.connector.binlog.source.BinlogSourceFactory
     */
    protected static String camelize(String pluginName, String suffix) {
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
            Options options, SyncConfig config, StreamExecutionEnvironment env) {
        DirtyConfig dirtyConfig = DirtyConfUtil.parse(options);

        Set<URL> urlSet = new HashSet<>();
        Set<URL> coreUrlSet =
                getJarFileDirPath("", config.getPluginRoot(), config.getRemotePluginPath(), "");
        Set<URL> formatsUrlSet =
                getJarFileDirPath(
                        FORMATS_SUFFIX, config.getPluginRoot(), config.getRemotePluginPath(), "");
        Set<URL> sourceUrlSet =
                getJarFileDirPath(
                        config.getReader().getName(),
                        config.getPluginRoot(),
                        config.getRemotePluginPath(),
                        CONNECTOR_DIR_NAME);
        Set<URL> sinkUrlSet =
                getJarFileDirPath(
                        config.getWriter().getName(),
                        config.getPluginRoot(),
                        config.getRemotePluginPath(),
                        CONNECTOR_DIR_NAME);
        Set<URL> metricUrlSet =
                getJarFileDirPath(
                        config.getMetricPluginConf().getPluginName(),
                        config.getPluginRoot(),
                        config.getRemotePluginPath(),
                        METRIC_SUFFIX);

        Set<URL> dirtyUrlSet =
                getJarFileDirPath(
                        dirtyConfig.getType(),
                        config.getPluginRoot(),
                        config.getRemotePluginPath(),
                        DIRTY_DATA_DIR_NAME);

        if (null != config.getCdcConf()) {
            Set<URL> restoreUrlSet = new HashSet<>();

            CacheConfig cache = config.getCdcConf().getCache();
            DDLConfig ddl = config.getCdcConf().getDdl();

            if (null != cache) {
                Set<URL> cacheUrlSet =
                        getJarFileDirPath(
                                cache.getType(),
                                config.getPluginRoot(),
                                config.getRemotePluginPath(),
                                RESTORE_DIR_NAME);
                restoreUrlSet.addAll(cacheUrlSet);
            }

            if (null != ddl) {
                Set<URL> ddlUrlSet =
                        getJarFileDirPath(
                                ddl.getType(),
                                config.getPluginRoot(),
                                config.getRemotePluginPath(),
                                RESTORE_DIR_NAME);
                restoreUrlSet.addAll(ddlUrlSet);
            }
            urlSet.addAll(restoreUrlSet);
        }

        String sourceConventName =
                RealTimeDataSourceNameUtil.getDataSourceName(config.getReader().getName());
        if (config.getNameMappingConfig() != null
                && StringUtils.isNotBlank(config.getNameMappingConfig().getSourceName())) {
            sourceConventName = config.getNameMappingConfig().getSourceName();
        }

        // 实时任务的sourceConventName和config里配置的名字是不一样的 否则就是离线任务
        if (!sourceConventName.equals(
                PluginUtil.replaceReaderAndWriterSuffix(config.getReader().getName()))) {
            String sinkConventName =
                    RealTimeDataSourceNameUtil.getDataSourceName(config.getWriter().getName());
            Set<URL> ddlConventSourceUrlSet =
                    getJarFileDirPath(
                            sourceConventName,
                            config.getPluginRoot(),
                            config.getRemotePluginPath(),
                            DDL_CONVENT_DIR_NAME);

            Set<URL> ddlConventSinkUrlSet =
                    getJarFileDirPath(
                            sinkConventName,
                            config.getPluginRoot(),
                            config.getRemotePluginPath(),
                            DDL_CONVENT_DIR_NAME);

            Set<URL> ddlConventBaseUrlSet =
                    getJarFileDirPath(
                            "",
                            config.getPluginRoot(),
                            config.getRemotePluginPath(),
                            DDL_CONVENT_DIR_NAME);

            urlSet.addAll(ddlConventSourceUrlSet);
            urlSet.addAll(ddlConventSinkUrlSet);
            urlSet.addAll(ddlConventBaseUrlSet);
        }

        urlSet.addAll(coreUrlSet);
        urlSet.addAll(formatsUrlSet);
        urlSet.addAll(sourceUrlSet);
        urlSet.addAll(sinkUrlSet);
        urlSet.addAll(metricUrlSet);
        urlSet.addAll(dirtyUrlSet);

        List<String> urlList = new ArrayList<>(urlSet.size());
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            Method add = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            add.setAccessible(true);

            int i = 0;
            for (URL url : urlSet) {
                String classFileName = String.format(CLASS_FILE_NAME_FMT, i);
                env.registerCachedFile(url.getPath(), classFileName, true);
                urlList.add(url.getPath());
                add.invoke(contextClassLoader, url);
                i++;
            }
        } catch (Exception e) {
            log.warn(
                    "cannot add core jar into contextClassLoader, coreUrlSet = {}",
                    GsonUtil.GSON.toJson(coreUrlSet),
                    e);
        }

        config.setSyncJarList(setPipelineOptionsToEnvConfig(env, urlList, options.getMode()));
    }

    public static List<String> registerPluginUrlToCachedFile(
            StreamExecutionEnvironment env, Set<URL> urlSet) throws Exception {
        List<String> urlList = new ArrayList<>(urlSet.size());
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Method add = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        add.setAccessible(true);
        // 类加载器泄露检查的时候可以把 jar 包都加载到 ChildFirst 的 ucp 的
        int i = env.getCachedFiles().size();
        Set<String> collect =
                env.getCachedFiles().stream().map(t -> t.f0).collect(Collectors.toSet());
        for (URL url : urlSet) {
            String classFileName = String.format(CLASS_FILE_NAME_FMT, i);
            for (int index = 0; index < 10; index++) {
                if (collect.contains(classFileName)) {
                    classFileName = String.format(CLASS_FILE_NAME_FMT, ++i);
                } else {
                    i++;
                }
            }
            env.registerCachedFile(url.getPath(), classFileName, true);
            urlList.add(url.getPath());
            add.invoke(contextClassLoader, url);
        }
        return urlList;
    }

    /**
     * 设置PipelineOptions.JARS, PipelineOptions.CLASSPATHS
     *
     * @param env
     * @param urlList
     * @return
     */
    public static List<String> setPipelineOptionsToEnvConfig(
            StreamExecutionEnvironment env, List<String> urlList, String executionMode) {
        try {
            Configuration configuration = (Configuration) env.getConfiguration();
            List<String> jarList = configuration.get(PipelineOptions.JARS);
            if (jarList == null) {
                jarList = new ArrayList<>(urlList.size());
            }
            jarList.addAll(urlList);

            List<String> pipelineJars = new ArrayList<>();

            List<String> classpathList = configuration.get(PipelineOptions.CLASSPATHS);
            if (classpathList == null) {
                classpathList = new ArrayList<>(urlList.size());
            }

            log.info("ChunJun executionMode: " + executionMode);
            if (ClusterMode.getByName(executionMode) == ClusterMode.kubernetesApplication) {
                for (String jarUrl : jarList) {
                    String newJarUrl = jarUrl;
                    if (StringUtils.startsWith(jarUrl, File.separator)) {
                        newJarUrl = "file:" + jarUrl;
                    }
                    if (!pipelineJars.contains(newJarUrl)) {
                        pipelineJars.add(newJarUrl);
                    }

                    if (!classpathList.contains(newJarUrl)) {
                        classpathList.add(newJarUrl);
                    }
                }
            } else {
                pipelineJars.addAll(jarList);
            }

            log.info("ChunJun reset pipeline.jars: " + pipelineJars);
            configuration.set(PipelineOptions.JARS, pipelineJars);

            configuration.set(PipelineOptions.CLASSPATHS, classpathList);
            return pipelineJars;
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
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
            log.warn("ClassLoader: {} is not instanceof URLClassLoader", contextClassLoader);
            return null;
        }
    }

    public static String replaceReaderAndWriterSuffix(String pluginName) {
        return pluginName
                .replace(READER_SUFFIX, "")
                .replace(SOURCE_SUFFIX, "")
                .replace(WRITER_SUFFIX, "")
                .replace(SINK_SUFFIX, "");
    }
}
