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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/3/18
 */
public class InputMetric {

    private Counter numRead;

    private RuntimeContext runtimeContext;

    public InputMetric(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;

        initMetric();
    }

    private void initMetric() {
        numRead = getRuntimeContext().getMetricGroup().counter(Metrics.NUM_READS);
    }

    public Counter getNumRead() {
        return numRead;
    }

    private RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }
}
