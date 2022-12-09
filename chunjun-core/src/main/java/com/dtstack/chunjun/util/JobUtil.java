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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class JobUtil {

    private JobUtil() throws IllegalAccessException {
        throw new IllegalAccessException(getClass().getName() + " can not be instantiated");
    }

    public static String replaceJobParameter(String pj, String job) {
        if (StringUtils.isNotBlank(pj)) {
            Map<String, String> parameters = commandJsonTransform(pj);
            return jsonValueReplace(job, parameters);
        }
        return job;
    }

    public static String jsonValueReplace(String json, Map<String, String> parameter) {
        for (String item : parameter.keySet()) {
            if (json.contains("${" + item + "}")) {
                json = json.replace("${" + item + "}", parameter.get(item));
            }
        }
        return json;
    }

    public static HashMap<String, String> commandJsonTransform(String command) {
        return JsonUtil.toObject(command, new TypeReference<HashMap<String, String>>() {});
    }
}
