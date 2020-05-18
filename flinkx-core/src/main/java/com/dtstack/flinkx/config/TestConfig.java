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
public class TestConfig extends AbstractConfig {

    private static final String KEY_FAILED_PER_RECORD = "failedPerRecord";
    private static final String KEY_ERROR_MSG = "errorMsg";
    private static final String KEY_SPEED_TEST = "speedTest";

    public TestConfig(Map<String, Object> map) {
        super(map);
    }

    public static TestConfig defaultConfig(){
        Map<String, Object> map = new HashMap<>(1);
        return new TestConfig(map);
    }

    public boolean errorTest() {
        return getIntVal(KEY_FAILED_PER_RECORD, -1) > 0;
    }

    public int getFailedPerRecord(){
        return getIntVal(KEY_FAILED_PER_RECORD, -1);
    }

    public String getErrorMsg(){
        return getStringVal(KEY_ERROR_MSG, "Error test");
    }

    public String getSpeedTest() {
        return getStringVal(KEY_SPEED_TEST, null);
    }
}
