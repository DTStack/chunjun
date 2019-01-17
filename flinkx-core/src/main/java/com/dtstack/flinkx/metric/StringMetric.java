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

package com.dtstack.flinkx.metric;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

/**
 * @author jiangbo
 * @explanation
 * @date 2019/1/12
 */
public class StringMetric implements Serializable {

    private String metricName;

    private RuntimeContext runtimeContext;

    private boolean toPrometheus = false;

    private StringAccumulator stringAccumulator;

    private StringGauge stringGauge;

    private StringMetric() {
    }

    public StringMetric(String metricName, RuntimeContext runtimeContext, boolean toPrometheus) {
        this.metricName = metricName;
        this.runtimeContext = runtimeContext;
        this.toPrometheus = toPrometheus;
    }

    public static StringMetric build(String metricName, RuntimeContext runtimeContext, boolean toPrometheus){
        StringMetric stringMetric = new StringMetric(metricName,runtimeContext,toPrometheus);
        stringMetric.init();
        return stringMetric;
    }

    public void init(){
        stringAccumulator = new StringAccumulator();
        runtimeContext.addAccumulator(metricName,stringAccumulator);

        if(toPrometheus){
            stringGauge = new StringGauge();
            runtimeContext.getMetricGroup().gauge(metricName,stringGauge);
        }
    }

    public void add(String value){
        stringAccumulator.add(value);

        if(toPrometheus){
            stringGauge.addValue(value);
        }
    }
}
