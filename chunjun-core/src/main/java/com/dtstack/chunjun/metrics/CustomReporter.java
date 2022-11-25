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

package com.dtstack.chunjun.metrics;

import com.dtstack.chunjun.config.MetricParam;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.CharacterFilter;

import java.util.regex.Pattern;

public abstract class CustomReporter {

    protected boolean makeTaskFailedWhenReportFailed;

    protected RuntimeContext context;

    protected static final Pattern UN_ALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
    protected static final CharacterFilter CHARACTER_FILTER =
            new CharacterFilter() {
                @Override
                public String filterCharacters(String input) {
                    return replaceInvalidChars(input);
                }
            };
    protected final CharacterFilter labelValueCharactersFilter = CHARACTER_FILTER;

    public CustomReporter(MetricParam metricParam) {
        this.context = metricParam.getContext();
        this.makeTaskFailedWhenReportFailed = metricParam.isMakeTaskFailedWhenReportFailed();
    }

    static String replaceInvalidChars(final String input) {
        // https://prometheus.io/docs/instrumenting/writing_exporters/
        // Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to
        // an underscore.
        return UN_ALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
    }

    /** init reporter */
    public abstract void open();

    /**
     * register metric
     *
     * @param accumulator
     * @param name
     */
    public abstract void registerMetric(Accumulator accumulator, String name);

    /** upload metrics */
    public abstract void report();

    /** close metric report */
    public abstract void close();
}
