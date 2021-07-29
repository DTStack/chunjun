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

package com.dtstack.flinkx.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Row Utilities
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class RowUtil {
    static Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    public static String rowToJson(Row row, String[] colName) {
        Preconditions.checkNotNull(colName);
        Map<String,Object> map = new LinkedHashMap<>(colName.length);

        for(int i = 0; i < colName.length; ++i) {
            String key = colName[i];
            Object value = row.getField(i);
            map.put(key, value);
        }

        return gson.toJson(map);
    }
}
