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
package com.dtstack.flinkx.connector.hive.util;

import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Date: 2021/06/22 Company: www.dtstack.com
 *
 * @author tudou
 */
public class PathConverterUtil {
    private static final Logger logger = LoggerFactory.getLogger(PathConverterUtil.class);
    private static final Pattern pat1 = Pattern.compile("\\$\\{.*?\\}");
    private static final String KEY_TABLE = "table";

    /**
     * @param path
     * @return
     */
    public static String regexByRules(
            Map<String, Object> output, String path, Map<String, String> distributeTableMapping) {
        try {
            Matcher mat1 = pat1.matcher(path);
            while (mat1.find()) {
                String pkey = mat1.group();
                String key = pkey.substring(2, pkey.length() - 1);
                Object value = output.get(key);
                if (value == null) {
                    value = "";
                }
                String ruleValue = value.toString();
                if (KEY_TABLE.equals(key)) {
                    ruleValue = distributeTableMapping.getOrDefault(ruleValue, ruleValue);
                }
                // .在sql中会视为db.table的分隔符，需要单独过滤特殊字符 '.'
                path = path.replace(pkey, ruleValue).replace(".", "_");
            }
        } catch (Exception e) {
            logger.error("parser path rules is fail", e);
        }
        return path;
    }

    public static String regexByRules(
            ColumnRowData columnRowData, String path, Map<String, String> distributeTableMapping) {
        try {
            if (columnRowData.getHeaders() == null) {
                return path;
            }
            Matcher mat1 = pat1.matcher(path);
            while (mat1.find()) {
                String pkey = mat1.group();
                String key = pkey.substring(2, pkey.length() - 1);
                AbstractBaseColumn baseColumn = columnRowData.getField(key);
                String ruleValue;
                if (baseColumn == null) {
                    ruleValue = "";
                } else {
                    ruleValue = baseColumn.asString();
                }
                if (KEY_TABLE.equals(key)) {
                    ruleValue = distributeTableMapping.getOrDefault(ruleValue, ruleValue);
                }
                // .在sql中会视为db.table的分隔符，需要单独过滤特殊字符 '.'
                path = path.replace(pkey, ruleValue).replace(".", "_");
            }
        } catch (Exception e) {
            logger.error("parser path rules is fail", e);
        }
        return path;
    }
}
