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

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;

import java.io.Serializable;

/**
 *
 * @author jiangbo
 * @date 2019/1/12
 */
public class CounterMetric implements Serializable {

    private String metricName;

    private RuntimeContext runtimeContext;

    private boolean toPrometheus = false;

    private LongCounter accumulatorCounter;

    private Counter metricCounter;

    private CounterMetric() {}

    private CounterMetric(String metricName, RuntimeContext runtimeContext,boolean toPrometheus) {
        this.metricName = metricName;
        this.runtimeContext = runtimeContext;
        this.toPrometheus = toPrometheus;
    }

    public static CounterMetric build(String metricName,RuntimeContext runtimeContext,boolean toPrometheus){
        CounterMetric counterMetric = new CounterMetric(metricName,runtimeContext,toPrometheus);
        counterMetric.init();
        return counterMetric;
    }

    public void init(){
        accumulatorCounter = runtimeContext.getLongCounter(metricName);

        if(toPrometheus){
            metricCounter = runtimeContext.getMetricGroup().counter(metricName);
        }
    }

    public void add(){
        accumulatorCounter.add(1);

        if(toPrometheus){
            metricCounter.inc();
        }
    }

    public void add(long n){
        accumulatorCounter.add(n);

        if(toPrometheus){
            metricCounter.inc(n);
        }
    }
}
