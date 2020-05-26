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

package com.dtstack.flinkx.config;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jiangbo
 * @explanation
 * @date 2020/3/11
 */
public class RestartConfig extends AbstractConfig {

    public static final String KEY_STRATEGY = "strategy";
    public static final String KEY_RESTART_ATTEMPTS = "restartAttempts";
    public static final String KEY_DELAY_INTERVAL = "delayInterval";
    public static final String KEY_FAILURE_RATE = "failureRate";
    public static final String KEY_FAILURE_INTERVAL = "failureInterval";

    public static final String STRATEGY_FIXED_DELAY = "fixedDelay";
    public static final String STRATEGY_FAILURE_RATE = "failureRate";
    public static final String STRATEGY_NO_RESTART = "NoRestart";

    public static final String DEFAULT_STRATEGY = STRATEGY_FIXED_DELAY;
    public static final int DEFAULT_RESTART_ATTEMPTS = 5;
    public static final int DEFAULT_DELAY_INTERVAL = 10;
    public static final int DEFAULT_FAILURE_RATE = 2;
    public static final int DEFAULT_FAILURE_INTERVAL = 60;

    public RestartConfig(Map<String, Object> map) {
        super(map);
    }

    public static RestartConfig defaultConfig(){
        Map<String, Object> map = new HashMap<>(1);
        return new RestartConfig(map);
    }

    public String getStrategy() {
        return getStringVal(KEY_STRATEGY, DEFAULT_STRATEGY);
    }

    public int getRestartAttempts(){
        return getIntVal(KEY_RESTART_ATTEMPTS, DEFAULT_RESTART_ATTEMPTS);
    }

    public int getDelayInterval() {
        return getIntVal(KEY_DELAY_INTERVAL, DEFAULT_DELAY_INTERVAL);
    }

    public int getFailureRate() {
        return getIntVal(KEY_FAILURE_RATE, DEFAULT_FAILURE_RATE);
    }

    public int getFailureInterval(){
        return getIntVal(KEY_FAILURE_INTERVAL, DEFAULT_FAILURE_INTERVAL);
    }
}
