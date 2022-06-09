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

import com.dtstack.chunjun.cdc.ddl.DdlConvent;
import com.dtstack.chunjun.cdc.monitor.MonitorConf;
import com.dtstack.chunjun.cdc.monitor.fetch.FetcherBase;
import com.dtstack.chunjun.cdc.monitor.store.StoreBase;
import com.dtstack.chunjun.classloader.ClassLoaderManager;
import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.conf.MetricParam;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.dirty.DirtyConf;
import com.dtstack.chunjun.dirty.consumer.DirtyDataCollector;
import com.dtstack.chunjun.enums.OperatorType;
import com.dtstack.chunjun.metrics.CustomReporter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.NoRestartException;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Service;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Iterator;
import java.util.Set;

/**
 * @program: chunjun
 * @author: wuren
 * @create: 2021/04/27
 */
public class DataSyncFactoryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(DataSyncFactoryUtil.class);

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
            ChunJunCommonConf commonConf,
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

    public static Pair<FetcherBase, StoreBase> discoverFetchBase(
            MonitorConf monitorConf, SyncConf syncConf) {
        try {
            String pluginType = monitorConf.getType();
            String storePluginClassName =
                    PluginUtil.getPluginClassName(pluginType, OperatorType.store);
            String fetcherPluginClassName =
                    PluginUtil.getPluginClassName(pluginType, OperatorType.fetcher);
            Set<URL> urlList =
                    PluginUtil.getJarFileDirPath(
                            pluginType, syncConf.getPluginRoot(), null, "restore-plugins");

            StoreBase store =
                    ClassLoaderManager.newInstance(
                            urlList,
                            cl -> {
                                Class<?> clazz = cl.loadClass(storePluginClassName);
                                Constructor<?> constructor =
                                        clazz.getConstructor(MonitorConf.class);
                                return (StoreBase) constructor.newInstance(monitorConf);
                            });
            FetcherBase fetcher =
                    ClassLoaderManager.newInstance(
                            urlList,
                            cl -> {
                                Class<?> clazz = cl.loadClass(fetcherPluginClassName);
                                Constructor<?> constructor =
                                        clazz.getConstructor(MonitorConf.class);
                                return (FetcherBase) constructor.newInstance(monitorConf);
                            });
            return Pair.of(fetcher, store);
        } catch (Exception e) {
            throw new NoRestartException("Load restore plugins failed!", e);
        }
    }

    public static FetcherBase discoverFetchBase(MonitorConf conf) {
        try {
            String pluginName = conf.getType();
            String pluginClassName =
                    PluginUtil.getPluginClassName(pluginName, OperatorType.fetcher);

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> clazz = classLoader.loadClass(pluginClassName);
            Constructor<?> constructor = clazz.getConstructor(MonitorConf.class);
            final FetcherBase fetcherBase = (FetcherBase) constructor.newInstance(conf);
            fetcherBase.openSubclass();
            return fetcherBase;
        } catch (Exception e) {
            throw new NoRestartException("Load dirty plugins failed!", e);
        }
    }

    public static DdlConvent discoverDdlConvent(String pluginType) {
        try {
            Iterator<DdlConvent> providers = Service.providers(DdlConvent.class);
            while (providers.hasNext()) {
                DdlConvent processor = providers.next();
                if (processor.getDataSourceType().equals(pluginType)) {
                    return processor;
                } else {
                    LOG.info(
                            "find ddl plugin and support dataSource is {}",
                            processor.getDataSourceType());
                }
            }
        } catch (Exception e) {
            throw new NoRestartException("Load ddl convent plugins failed!", e);
        }
        throw new NoRestartException("not found ddl convent plugin!,plugin type is " + pluginType);
    }
}
