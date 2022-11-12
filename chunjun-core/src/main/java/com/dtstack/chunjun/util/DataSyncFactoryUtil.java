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

import com.dtstack.chunjun.cdc.CdcConf;
import com.dtstack.chunjun.cdc.conf.CacheConf;
import com.dtstack.chunjun.cdc.conf.DDLConf;
import com.dtstack.chunjun.cdc.ddl.DdlConvent;
import com.dtstack.chunjun.cdc.handler.CacheHandler;
import com.dtstack.chunjun.cdc.handler.DDLHandler;
import com.dtstack.chunjun.classloader.ClassLoaderManager;
import com.dtstack.chunjun.conf.CommonConfig;
import com.dtstack.chunjun.conf.MetricParam;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.dirty.DirtyConf;
import com.dtstack.chunjun.dirty.consumer.DirtyDataCollector;
import com.dtstack.chunjun.enums.OperatorType;
import com.dtstack.chunjun.mapping.MappingConf;
import com.dtstack.chunjun.metrics.CustomReporter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.NoRestartException;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Set;

/**
 * @program: chunjun
 * @author: wuren
 * @create: 2021/04/27
 */
public class DataSyncFactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DataSyncFactoryUtil.class);

    private static final String DEFAULT_DIRTY_TYPE = "default";

    private static final String DEFAULT_DIRTY_CLASS =
            "com.dtstack.chunjun.dirty.consumer.DefaultDirtyDataCollector";

    public static SourceFactory discoverSource(SyncConf config, StreamExecutionEnvironment env) {
        try {
            String pluginName = config.getJob().getReader().getName();
            String pluginClassName = PluginUtil.getPluginClassName(pluginName, OperatorType.source);
            return ClassLoaderManager.newInstance(
                    config.getSyncJarList(),
                    cl -> {
                        Class<?> clazz = cl.loadClass(pluginClassName);
                        Constructor<?> constructor =
                                clazz.getConstructor(
                                        SyncConf.class, StreamExecutionEnvironment.class);
                        return (SourceFactory) constructor.newInstance(config, env);
                    });
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    public static SinkFactory discoverSink(SyncConf config) {
        try {
            String pluginName = config.getJob().getContent().get(0).getWriter().getName();
            String pluginClassName = PluginUtil.getPluginClassName(pluginName, OperatorType.sink);
            return ClassLoaderManager.newInstance(
                    config.getSyncJarList(),
                    cl -> {
                        Class<?> clazz = cl.loadClass(pluginClassName);
                        Constructor<?> constructor = clazz.getConstructor(SyncConf.class);
                        return (SinkFactory) constructor.newInstance(config);
                    });
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    public static CustomReporter discoverMetric(
            CommonConfig commonConf,
            RuntimeContext context,
            boolean makeTaskFailedWhenReportFailed) {
        try {
            String pluginName = commonConf.getMetricPluginName();
            String pluginClassName = PluginUtil.getPluginClassName(pluginName, OperatorType.metric);
            MetricParam metricParam =
                    new MetricParam(
                            context, makeTaskFailedWhenReportFailed, commonConf.getMetricProps());

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> clazz = classLoader.loadClass(pluginClassName);
            Constructor<?> constructor = clazz.getConstructor(MetricParam.class);

            return (CustomReporter) constructor.newInstance(metricParam);
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    public static DirtyDataCollector discoverDirty(DirtyConf conf) {
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

    public static DDLHandler discoverDdlHandler(DDLConf ddlConf) {
        try {
            String pluginClassName =
                    PluginUtil.getPluginClassName(ddlConf.getType(), OperatorType.ddl);

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> clazz = classLoader.loadClass(pluginClassName);
            Constructor<?> constructor = clazz.getConstructor(DDLConf.class);
            return (DDLHandler) constructor.newInstance(ddlConf);
        } catch (Exception e) {
            throw new NoRestartException("Load ddlHandler plugins failed!", e);
        }
    }

    public static DDLHandler discoverDdlHandler(CdcConf cdcConf, SyncConf syncConf) {
        try {
            DDLConf ddl = cdcConf.getDdl();
            String ddlPluginClassName =
                    PluginUtil.getPluginClassName(ddl.getType(), OperatorType.ddl);

            Set<URL> ddlList =
                    PluginUtil.getJarFileDirPath(
                            ddl.getType(), syncConf.getPluginRoot(), null, "restore-plugins");

            return ClassLoaderManager.newInstance(
                    ddlList,
                    cl -> {
                        Class<?> clazz = cl.loadClass(ddlPluginClassName);
                        Constructor<?> constructor = clazz.getConstructor(DDLConf.class);
                        return (DDLHandler) constructor.newInstance(ddl);
                    });
        } catch (Exception e) {
            throw new NoRestartException("Load restore plugins failed!", e);
        }
    }

    public static CacheHandler discoverCacheHandler(CdcConf cdcConf, SyncConf syncConf) {
        try {
            CacheConf cache = cdcConf.getCache();

            String cachePluginClassName =
                    PluginUtil.getPluginClassName(cache.getType(), OperatorType.cache);

            Set<URL> cacheList =
                    PluginUtil.getJarFileDirPath(
                            cache.getType(), syncConf.getPluginRoot(), null, "restore-plugins");

            return ClassLoaderManager.newInstance(
                    cacheList,
                    cl -> {
                        Class<?> clazz = cl.loadClass(cachePluginClassName);
                        Constructor<?> constructor = clazz.getConstructor(CacheConf.class);
                        return (CacheHandler) constructor.newInstance(cache);
                    });
        } catch (Exception e) {
            throw new NoRestartException("Load restore plugins failed!", e);
        }
    }

    public static DdlConvent discoverDdlConventHandler(
            MappingConf mappingConf, String pluginName, SyncConf syncConf) {
        try {

            String cachePluginClassName =
                    PluginUtil.getPluginClassName(pluginName, OperatorType.ddlConvent);

            Set<URL> cacheList =
                    PluginUtil.getJarFileDirPath(
                            pluginName, syncConf.getPluginRoot(), null, "ddl-plugins");

            return ClassLoaderManager.newInstance(
                    cacheList,
                    cl -> {
                        Class<?> clazz = cl.loadClass(cachePluginClassName);
                        Constructor<?> constructor = clazz.getConstructor(MappingConf.class);
                        return (DdlConvent) constructor.newInstance(mappingConf);
                    });
        } catch (Exception e) {
            throw new NoRestartException("Load ddlConvent failed!", e);
        }
    }
}
