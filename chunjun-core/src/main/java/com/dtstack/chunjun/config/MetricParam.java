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

package com.dtstack.chunjun.config;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.Map;
import java.util.StringJoiner;

public class MetricParam {

    private RuntimeContext context;
    private boolean makeTaskFailedWhenReportFailed;
    private Map<String, Object> metricPluginConf;

    public MetricParam(
            RuntimeContext context,
            boolean makeTaskFailedWhenReportFailed,
            Map<String, Object> metricPluginConf) {
        this.context = context;
        this.makeTaskFailedWhenReportFailed = makeTaskFailedWhenReportFailed;
        this.metricPluginConf = metricPluginConf;
    }

    public RuntimeContext getContext() {
        return context;
    }

    public void setContext(RuntimeContext context) {
        this.context = context;
    }

    public boolean isMakeTaskFailedWhenReportFailed() {
        return makeTaskFailedWhenReportFailed;
    }

    public void setMakeTaskFailedWhenReportFailed(boolean makeTaskFailedWhenReportFailed) {
        this.makeTaskFailedWhenReportFailed = makeTaskFailedWhenReportFailed;
    }

    public Map<String, Object> getMetricPluginConf() {
        return metricPluginConf;
    }

    public void setMetricPluginConf(Map<String, Object> metricPluginConf) {
        this.metricPluginConf = metricPluginConf;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MetricParam.class.getSimpleName() + "[", "]")
                .add("context=" + context)
                .add("makeTaskFailedWhenReportFailed=" + makeTaskFailedWhenReportFailed)
                .add("metricPluginConfig=" + metricPluginConf)
                .toString();
    }
}
