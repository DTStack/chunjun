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

package com.dtstack.chunjun.util;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

@Slf4j
public class PrintUtil {

    public static String printResult(Map<String, Object> result) {
        List<String> names = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        result.forEach(
                (name, val) -> {
                    names.add(name);
                    values.add(String.valueOf(val));
                });

        int maxLength = 0;
        for (String name : names) {
            maxLength = Math.max(maxLength, name.length());
        }
        maxLength += 5;

        StringBuilder builder = new StringBuilder(128);
        builder.append("\n*********************************************\n");
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            builder.append(name)
                    .append(StringUtils.repeat(" ", maxLength - name.length()))
                    .append("|  ")
                    .append(values.get(i));

            if (i + 1 < names.size()) {
                builder.append("\n");
            }
        }
        builder.append("\n*********************************************\n");
        log.info(builder.toString());
        return builder.toString();
    }
}
