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

import com.dtstack.chunjun.cdc.CdcConfig;
import com.dtstack.chunjun.cdc.config.CacheConfig;
import com.dtstack.chunjun.cdc.config.DDLConfig;
import com.dtstack.chunjun.cdc.ddl.DdlConvent;
import com.dtstack.chunjun.cdc.handler.CacheHandler;
import com.dtstack.chunjun.cdc.handler.DDLHandler;
import com.dtstack.chunjun.classloader.ClassLoaderManager;
import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.MetricParam;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.dirty.DirtyConfig;
import com.dtstack.chunjun.dirty.consumer.DirtyDataCollector;
import com.dtstack.chunjun.enums.OperatorType;
import com.dtstack.chunjun.mapping.MappingConfig;
import com.dtstack.chunjun.metrics.CustomReporter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.NoRestartException;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Set;

public class DataSyncFactoryUtil {

    private static final String DEFAULT_DIRTY_TYPE = "default";

    private static final String DEFAULT_DIRTY_CLASS =
            "com.dtstack.chunjun.dirty.consumer.DefaultDirtyDataCollector";

    public static SourceFactory discoverSource(SyncConfig config, StreamExecutionEnvironment env) {
        try {
            String pluginName = config.getJob().getReader().getName();
            String pluginClassName = PluginUtil.getPluginClassName(pluginName, OperatorType.source);
            return ClassLoaderManager.newInstance(
                    config.getSyncJarList(),
                    cl -> {
                        Class<?> clazz = cl.loadClass(pluginClassName);
                        Constructor<?> constructor =
                                clazz.getConstructor(
                                        SyncConfig.class, StreamExecutionEnvironment.class);
                        return (SourceFactory) constructor.newInstance(config, env);
                    });
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    public static SinkFactory discoverSink(SyncConfig config) {
        try {
            String pluginName = config.getJob().getContent().get(0).getWriter().getName();
            String pluginClassName = PluginUtil.getPluginClassName(pluginName, OperatorType.sink);
            return ClassLoaderManager.newInstance(
                    config.getSyncJarList(),
                    cl -> {
                        Class<?> clazz = cl.loadClass(pluginClassName);
                        Constructor<?> constructor = clazz.getConstructor(SyncConfig.class);
                        return (SinkFactory) constructor.newInstance(config);
                    });
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    public static CustomReporter discoverMetric(
            CommonConfig commonConfig,
            RuntimeContext context,
            boolean makeTaskFailedWhenReportFailed) {
        try {
            String pluginName = commonConfig.getMetricPluginName();
            String pluginClassName = PluginUtil.getPluginClassName(pluginName, OperatorType.metric);
            MetricParam metricParam =
                    new MetricParam(
                            context, makeTaskFailedWhenReportFailed, commonConfig.getMetricProps());

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> clazz = classLoader.loadClass(pluginClassName);
            Constructor<?> constructor = clazz.getConstructor(MetricParam.class);

            return (CustomReporter) constructor.newInstance(metricParam);
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    public static DirtyDataCollector discoverDirty(DirtyConfig conf) {
        try {
            String pluginName = conf.getType();
            String pluginClassName = PluginUtil.getPluginClassName(pluginName, OperatorType.dirty);

            if (pluginName.equals(DEFAULT_DIRTY_TYPE)) {
                pluginClassName = DEFAULT_DIRTY_CLASS;
            }

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> clazz = classLoader.loadClass(pluginClassName);
            Constructor<?> constructor = clazz.getConstructor();
            final DirtyDataCollector consumer = (DirtyDataCollector) constructor.newInstance();
            consumer.initializeConsumer(conf);
            return consumer;
        } catch (Exception e) {
            throw new NoRestartException("Load dirty plugins failed!", e);
        }
    }

    public static DDLHandler discoverDdlHandler(DDLConfig ddlConfig) {
        try {
            String pluginClassName =
                    PluginUtil.getPluginClassName(ddlConfig.getType(), OperatorType.ddl);

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> clazz = classLoader.loadClass(pluginClassName);
            Constructor<?> constructor = clazz.getConstructor(DDLConfig.class);
            return (DDLHandler) constructor.newInstance(ddlConfig);
        } catch (Exception e) {
            throw new NoRestartException("Load ddlHandler plugins failed!", e);
        }
    }

    public static DDLHandler discoverDdlHandler(CdcConfig cdcConfig, SyncConfig syncConfig) {
        try {
            DDLConfig ddl = cdcConfig.getDdl();
            String ddlPluginClassName =
                    PluginUtil.getPluginClassName(ddl.getType(), OperatorType.ddl);

            Set<URL> ddlList =
                    PluginUtil.getJarFileDirPath(
                            ddl.getType(), syncConfig.getPluginRoot(), null, "restore-plugins");

            return ClassLoaderManager.newInstance(
                    ddlList,
                    cl -> {
                        Class<?> clazz = cl.loadClass(ddlPluginClassName);
                        Constructor<?> constructor = clazz.getConstructor(DDLConfig.class);
                        return (DDLHandler) constructor.newInstance(ddl);
                    });
        } catch (Exception e) {
            throw new NoRestartException("Load restore plugins failed!", e);
        }
    }

    public static CacheHandler discoverCacheHandler(CdcConfig cdcConfig, SyncConfig syncConfig) {
        try {
            CacheConfig cache = cdcConfig.getCache();

            String cachePluginClassName =
                    PluginUtil.getPluginClassName(cache.getType(), OperatorType.cache);

            Set<URL> cacheList =
                    PluginUtil.getJarFileDirPath(
                            cache.getType(), syncConfig.getPluginRoot(), null, "restore-plugins");

            return ClassLoaderManager.newInstance(
                    cacheList,
                    cl -> {
                        Class<?> clazz = cl.loadClass(cachePluginClassName);
                        Constructor<?> constructor = clazz.getConstructor(CacheConfig.class);
                        return (CacheHandler) constructor.newInstance(cache);
                    });
        } catch (Exception e) {
            throw new NoRestartException("Load restore plugins failed!", e);
        }
    }

    public static DdlConvent discoverDdlConventHandler(
            MappingConfig mappingConfig, String pluginName, SyncConfig syncConfig) {
        try {

            String cachePluginClassName =
                    PluginUtil.getPluginClassName(pluginName, OperatorType.ddlConvent);

            Set<URL> cacheList =
                    PluginUtil.getJarFileDirPath(
                            pluginName, syncConfig.getPluginRoot(), null, "ddl-plugins");

            return ClassLoaderManager.newInstance(
                    cacheList,
                    cl -> {
                        Class<?> clazz = cl.loadClass(cachePluginClassName);
                        Constructor<?> constructor = clazz.getConstructor(MappingConfig.class);
                        return (DdlConvent) constructor.newInstance(mappingConfig);
                    });
        } catch (Exception e) {
            throw new NoRestartException("Load ddlConvent failed!", e);
        }
    }
}
