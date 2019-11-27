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


package com.dtstack.flinkx.metrics;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * @author jiangbo
 * @date 2019/11/27
 */
public class MetricReporterHandler {

    protected static final Logger LOG = LoggerFactory.getLogger(MetricReporterHandler.class);

    /**
     * 在任务结束的时候或者需要的时候可以调用这个方法把metrics输出，比如把metrics输出到pushgateway
     */
    public static void reportMetrics(RuntimeContext runtimeContext) {
        try {
            MetricGroup mgObj = runtimeContext.getMetricGroup();
            Class<AbstractMetricGroup> amgCls = (Class<AbstractMetricGroup>) mgObj.getClass().getSuperclass().getSuperclass();
            Field registryField = amgCls.getDeclaredField("registry");
            registryField.setAccessible(true);
            MetricRegistryImpl registryImplObj = (MetricRegistryImpl) registryField.get(mgObj);
            if (registryImplObj.getReporters().isEmpty()) {
                LOG.info("No Reporter config");
                return;
            }

            for (MetricReporter reporter : registryImplObj.getReporters()) {
                if(reporter instanceof Scheduled){
                    ((Scheduled) reporter).report();
                    LOG.info("Report metrics with reporter:[{}]", reporter.getClass());
                }
            }
        } catch (Exception e) {
            LOG.warn("Report metric failed", e);
        }
    }
}
