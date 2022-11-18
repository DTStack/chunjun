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

package com.dtstack.chunjun.connector.influxdb.enums;

import java.util.concurrent.TimeUnit;

public enum TimePrecisionEnums {
    NS("NS", TimeUnit.NANOSECONDS),
    U("U", TimeUnit.MICROSECONDS),
    MS("MS", TimeUnit.MILLISECONDS),
    S("S", TimeUnit.SECONDS),
    M("M", TimeUnit.MINUTES),
    H("H", TimeUnit.HOURS);

    private final String desc;

    private final TimeUnit precision;

    public TimeUnit getPrecision() {
        return this.precision;
    }

    public static TimePrecisionEnums of(String desc) {
        for (TimePrecisionEnums precision : TimePrecisionEnums.values()) {
            if (precision.desc.equalsIgnoreCase(desc)) return precision;
        }
        return TimePrecisionEnums.NS;
    }

    TimePrecisionEnums(String desc, TimeUnit precision) {
        this.desc = desc;
        this.precision = precision;
    }
}
