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

package com.dtstack.flinkx.dirty.utils;

import com.dtstack.flinkx.dirty.impl.DirtyDataEntry;

import org.slf4j.Logger;

/**
 * @author tiezhu@dtstack
 * @date 22/09/2021 Wednesday
 */
public class LogUtil {

    private LogUtil() {}

    public static void warn(
            Logger log, String message, Throwable cause, long rate, long currentNumber) {
        if (currentNumber % rate == 0) {
            if (cause == null) {
                log.warn(message);
            } else {
                log.warn(message, cause);
            }
        }
    }

    public static void printDirty(Logger log, DirtyDataEntry dirty, long rate, long currentNumber) {
        if (currentNumber % rate == 0) {
            log.warn("Get dirty data.");
            log.warn(dirty.toString());
        }
    }
}
