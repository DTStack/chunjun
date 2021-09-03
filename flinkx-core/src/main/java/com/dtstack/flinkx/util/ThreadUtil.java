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

package com.dtstack.flinkx.util;

import java.util.concurrent.TimeUnit;

/** @author tiezhu Date 2020-12-25 Company dtstack */
public class ThreadUtil {
    public static final Long DEFAULT_SLEEP_TIME = 10L;

    public static void sleepSeconds(long timeout) {
        try {
            TimeUnit.SECONDS.sleep(timeout);
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    public static void sleepMinutes(long timeout) {
        try {
            TimeUnit.MINUTES.sleep(timeout);
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    public static void sleepMicroseconds(long timeout) {
        try {
            TimeUnit.MICROSECONDS.sleep(timeout);
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

    public static void sleepMilliseconds(long timeout) {
        try {
            TimeUnit.MILLISECONDS.sleep(timeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
