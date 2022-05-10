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

package com.dtstack.chunjun.util;

import org.slf4j.Logger;

/**
 * @program: flinkStreamSQL
 * @author: wuren
 * @create: 2021/01/19
 */
public class SampleUtils {

    private static int samplingIntervalCount = 0;

    /**
     * static变量无法序列化到TaskManager，所以设定此方法，赋值给每个具体使用的类。
     *
     * @return
     */
    public static int getSamplingIntervalCount() {
        return samplingIntervalCount;
    }

    public static void setSamplingIntervalCount(int interval) {
        samplingIntervalCount = interval;
    }

    public static void samplingSourcePrint(
            int samplingIntervalCount, Logger logger, long count, String message) {
        Runnable func = () -> logger.info("sampling source input data: " + message);
        samplingPrint(samplingIntervalCount, count, func);
    }

    public static void samplingSinkPrint(
            int samplingIntervalCount, Logger logger, long count, String message) {
        Runnable func = () -> logger.info("sampling sink output data: " + message);
        samplingPrint(samplingIntervalCount, count, func);
    }

    public static void samplingDirtyPrint(
            int samplingIntervalCount, Logger logger, long count, String message) {
        Runnable func = () -> logger.info("sampling dirty output data: " + message);
        samplingPrint(samplingIntervalCount, count, func);
    }

    private static void samplingPrint(int samplingIntervalCount, long count, Runnable func) {
        if (samplingIntervalCount > 0 && count % samplingIntervalCount == 0) {
            func.run();
        }
    }
}
