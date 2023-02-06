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

package com.dtstack.chunjun.connector.http.util;

import org.apache.commons.lang3.StringUtils;

public class JsonPathUtil {
    public static String parseJsonPath(String path) {
        //
        if (path.startsWith("$.")) {
            return path.substring(2);
        } else if (path.startsWith("${") && path.endsWith("}")) {
            return path.substring(2, path.length() - 1);
        }
        throw new IllegalArgumentException("just support parse ${xxx.xxx} or $.xxx.xx format");
    }

    public static boolean isJsonPath(String path) {
        if (StringUtils.isBlank(path)) {
            throw new IllegalArgumentException("path is not empty");
        }
        return path.startsWith("$.") || (path.startsWith("${") && path.endsWith("}"));
    }
}
