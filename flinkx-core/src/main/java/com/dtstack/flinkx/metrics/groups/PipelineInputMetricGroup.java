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

package com.dtstack.flinkx.metrics.groups;


import com.dtstack.flinkx.constants.Metrics;
import com.dtstack.flinkx.metrics.MetricRegistry;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.MetricNames;
import com.dtstack.flinkx.metrics.scope.ScopeFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Metric group that contains shareable pre-defined IO-related metrics. The metrics registration is
 * forwarded to the parent task metric group.
 */
public class PipelineInputMetricGroup<C extends ComponentMetricGroup<C>> extends ComponentMetricGroup {

    private final Counter numBytesInLocal;
    private final Meter numBytesInRateLocal;

    private final SumCounter numRead;


    private final String hostname;
    private final String pluginType;
    private final String pluginName;
    private final String jobName;

    public PipelineInputMetricGroup(MetricRegistry registry,
                                    String hostname,
                                    String pluginType,
                                    String pluginName,
                                    String jobName) {
        super(registry, registry.getScopeFormats().getPipelineScopeFormat().formatScope(hostname, pluginType, pluginName, jobName), null);

        this.hostname = hostname;
        this.pluginType = pluginType;
        this.pluginName = pluginName;
        this.jobName = jobName;

        this.numBytesInLocal = counter(MetricNames.IO_NUM_BYTES_IN_LOCAL);
        this.numBytesInRateLocal = meter(MetricNames.IO_NUM_BYTES_IN_LOCAL_RATE, new MeterView(numBytesInLocal, 60));

        this.numRead = (SumCounter) counter(Metrics.NUM_READS, new SumCounter());
    }

    // ============================================================================================
    // Getters
    // ============================================================================================


    public Counter getNumBytesInLocal() {
        return numBytesInLocal;
    }

    public Meter getNumBytesInRateLocal() {
        return numBytesInRateLocal;
    }

    public SumCounter getNumRead() {
        return numRead;
    }

    @Override
    protected void putVariables(Map variables) {
        variables.put(ScopeFormat.SCOPE_HOST, hostname);
        variables.put(ScopeFormat.SCOPE_PLUGINE_TYPE, pluginType);
        variables.put(ScopeFormat.SCOPE_PLUGINE_NAME, pluginName);
        variables.put(ScopeFormat.SCOPE_JOB_NAME, jobName);
    }

    @Override
    protected Iterable<? extends ComponentMetricGroup> subComponents() {
        return null;
    }

    @Override
    protected String getGroupName(CharacterFilter filter) {
        return "pipeline";
    }

    // ============================================================================================
    // Metric Reuse
    // ============================================================================================
    public void reuseRecordsInputCounter(Counter numRecordsInCounter) {
        this.numRead.addCounter(numRecordsInCounter);
    }

    /**
     * A {@link SimpleCounter} that can contain other {@link Counter}s. A call to {@link SumCounter#getCount()} returns
     * the sum of this counters and all contained counters.
     */
    private static class SumCounter extends SimpleCounter {
        private final List<Counter> internalCounters = new ArrayList<>();

        SumCounter() {
        }

        public void addCounter(Counter toAdd) {
            internalCounters.add(toAdd);
        }

        @Override
        public long getCount() {
            long sum = super.getCount();
            for (Counter counter : internalCounters) {
                sum += counter.getCount();
            }
            return sum;
        }
    }
}
