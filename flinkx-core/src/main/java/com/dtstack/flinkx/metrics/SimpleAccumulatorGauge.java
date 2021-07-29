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

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.metrics.Gauge;

import java.io.Serializable;

/**
 * company: www.dtstack.com
 *
 * @author: toutian
 * create: 2019/3/21
 */
public class SimpleAccumulatorGauge<T extends Serializable> implements Gauge<T> {

    private Accumulator<T, T> accumulator;

    public SimpleAccumulatorGauge(Accumulator<T, T> accumulator) {
        this.accumulator = accumulator;
    }

    @Override
    public T getValue() {
        return accumulator.getLocalValue();
    }
}
