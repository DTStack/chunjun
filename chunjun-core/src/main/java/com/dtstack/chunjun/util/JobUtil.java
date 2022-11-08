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

import com.dtstack.chunjun.constants.ConstantValue;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class JobUtil {

    private JobUtil() throws IllegalAccessException {
        throw new IllegalAccessException(getClass().getName() + " can not be instantiated");
    }

    public static String replaceJobParameter(String p, String job) {
        if (StringUtils.isNotBlank(p)) {
            Map<String, String> parameters = CommandTransform(p);
            job = JsonValueReplace(job, parameters);
        }
        return job;
    }

    public static String JsonValueReplace(String json, Map<String, String> parameter) {
        for (String item : parameter.keySet()) {
            if (json.contains("${" + item + "}")) {
                json = json.replace("${" + item + "}", parameter.get(item));
            }
        }
        return json;
    }

    /**
     * @param command
     * @return HashMap
     * @description: convert params after '-p' in shell to hashmap
     */
    public static HashMap<String, String> CommandTransform(String command) {
        HashMap<String, String> parameter = new HashMap<>();
        String[] split = StringUtils.split(command, ConstantValue.COMMA_SYMBOL);
        for (int i = 0; i < split.length; i++) {
            String[] params = split[i].split(ConstantValue.EQUAL_SYMBOL);
            if (params[0].trim().startsWith(ConstantValue.PARAMS_SEP)) {
                StringBuilder sb = new StringBuilder();
                sb.append(params[1]);
                while (i + 1 < split.length && !split[i + 1].contains(ConstantValue.EQUAL_SYMBOL)) {
                    sb.append(ConstantValue.COMMA_SYMBOL + split[i + 1]);
                    i++;
                }
                String key =
                        params[0].trim().replace(ConstantValue.PARAMS_SEP, ConstantValue.EMPTY_STR);
                parameter.put(key, sb.toString());
            } else {
                parameter.put(params[0].trim(), params[1].trim());
            }
        }
        return parameter;
    }
}
