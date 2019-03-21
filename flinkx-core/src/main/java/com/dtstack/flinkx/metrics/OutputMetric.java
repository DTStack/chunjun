/**
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

import com.dtstack.flinkx.constants.Metrics;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;

/**
 * company: www.dtstack.com
 *
 * @author: toutian
 * create: 2019/3/18
 */
public class OutputMetric {

    private transient RuntimeContext runtimeContext;

    public OutputMetric(RuntimeContext runtimeContext, IntCounter numErrors, IntCounter numNullErrors,
                        IntCounter numDuplicateErrors, IntCounter numConversionErrors, IntCounter numOtherErrors, LongCounter numWrite) {
        this.runtimeContext = runtimeContext;

        final MetricGroup flinkxOutput = getRuntimeContext().getMetricGroup().addGroup(Metrics.METRIC_GROUP_KEY_FLINKX, Metrics.METRIC_GROUP_VALUE_OUTPUT);

        flinkxOutput.gauge(Metrics.NUM_ERRORS, new SimpleAccumulatorGauge<Integer>(numErrors));
        flinkxOutput.gauge(Metrics.NUM_NULL_ERRORS, new SimpleAccumulatorGauge<Integer>(numNullErrors));
        flinkxOutput.gauge(Metrics.NUM_DUPLICATE_ERRORS, new SimpleAccumulatorGauge<Integer>(numDuplicateErrors));
        flinkxOutput.gauge(Metrics.NUM_CONVERSION_ERRORS, new SimpleAccumulatorGauge<Integer>(numConversionErrors));
        flinkxOutput.gauge(Metrics.NUM_OTHER_ERRORS, new SimpleAccumulatorGauge<Integer>(numOtherErrors));
        flinkxOutput.gauge(Metrics.NUM_WRITES, new SimpleAccumulatorGauge<Long>(numWrite));
    }

    private RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }
}
