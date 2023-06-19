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

package com.dtstack.chunjun.config;

import org.apache.flink.api.common.functions.RuntimeContext;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MetricParamTest {

    /** Should return the correct string */
    @Test
    public void toStringShouldReturnCorrectString() {
        Map<String, Object> metricPluginConf = new HashMap<>();
        metricPluginConf.put("pluginName", "prometheus");
        metricPluginConf.put("rowSizeCalculatorType", "objectSizeCalculator");
        metricPluginConf.put("pluginProp", new HashMap<>());

        MetricParam metricParam = new MetricParam(null, true, metricPluginConf);

        assertEquals(
                "MetricParam(context=null, makeTaskFailedWhenReportFailed=true, metricPluginConf={pluginName=prometheus, pluginProp={}, rowSizeCalculatorType=objectSizeCalculator})",
                metricParam.toString());
    }

    /** Should return the metricPluginConf */
    @Test
    public void getMetricPluginConfShouldReturnTheMetricPluginConf() {
        Map<String, Object> metricPluginConf = new HashMap<>();
        metricPluginConf.put("pluginName", "prometheus");
        metricPluginConf.put("rowSizeCalculatorType", "objectSizeCalculator");
        metricPluginConf.put("pluginProp", new HashMap<>());

        MetricParam metricParam = new MetricParam(null, false, metricPluginConf);

        Map<String, Object> actualMetricPluginConf = metricParam.getMetricPluginConf();

        assertEquals("prometheus", actualMetricPluginConf.get("pluginName"));
        assertEquals("objectSizeCalculator", actualMetricPluginConf.get("rowSizeCalculatorType"));
        assertEquals(new HashMap<>(), actualMetricPluginConf.get("pluginProp"));
    }

    /** Should return true when makeTaskFailedWhenReportFailed is true */
    @Test
    public void
            isMakeTaskFailedWhenReportFailedShouldReturnTrueWhenMakeTaskFailedWhenReportFailedIsTrue() {
        Map<String, Object> metricPluginConf = new HashMap<>();
        MetricParam metricParam = new MetricParam(null, true, metricPluginConf);
        assertTrue(metricParam.isMakeTaskFailedWhenReportFailed());
    }

    /** Should return false when makeTaskFailedWhenReportFailed is false */
    @Test
    public void
            isMakeTaskFailedWhenReportFailedShouldReturnFalseWhenMakeTaskFailedWhenReportFailedIsFalse() {
        Map<String, Object> metricPluginConf = new HashMap<>();
        MetricParam metricParam = new MetricParam(null, false, metricPluginConf);
        assertFalse(metricParam.isMakeTaskFailedWhenReportFailed());
    }

    /** Should return the context */
    @Test
    public void getContextShouldReturnTheContext() {
        RuntimeContext context = mock(RuntimeContext.class);
        MetricParam metricParam = new MetricParam(context, false, new HashMap<>());
        assertEquals(context, metricParam.getContext());
    }
}
