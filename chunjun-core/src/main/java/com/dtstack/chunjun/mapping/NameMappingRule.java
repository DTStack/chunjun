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

package com.dtstack.chunjun.mapping;

import org.apache.commons.collections.MapUtils;

import java.io.Serializable;
import java.util.Map;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/23 星期四
 */
public class NameMappingRule implements Serializable {

    private static final long serialVersionUID = 1L;
    public static final String KEY_BEFORE = "before_";
    public static final String KEY_AFTER = "after_";

    private final Map<String, String> schemaMappings;
    private final Map<String, Object> tableMappings;
    private final Map<String, Object> fieldMappings;

    public NameMappingRule(NameMappingConf conf) {
        this.schemaMappings = conf.getSchemaMappings();
        this.tableMappings = conf.getTableMappings();
        this.fieldMappings = conf.getFieldMappings();
    }

    public String schemaMapping(String original) {
        return mapping(original, schemaMappings);
    }

    public String tableMapping(String schema, String original) {
        if (tableMappings.containsKey(schema)) {
            return mapping(original, (Map<String, String>) tableMappings.get(schema));
        }
        return original;
    }

    public Map<String, String> getMapFields(String schema, String table) {
        if (fieldMappings.containsKey(schema)) {
            Map<String, Map<String, String>> tableFields =
                    (Map<String, Map<String, String>>) fieldMappings.get(schema);
            if (tableFields.containsKey(table)) {
                return tableFields.get(table);
            }
        }
        return null;
    }

    public String mapping(String original, Map<String, String> mappings) {
        if (mappings.containsKey(original)) {
            return mappings.get(original);
        }
        return original;
    }

    public String fieldMapping(String original, Map<String, String> mappings) {
        if (original.startsWith(KEY_BEFORE)) {
            String trueCol = original.substring(7);
            String targetCol = mappings.get(trueCol);
            if (targetCol != null) {
                return KEY_BEFORE + targetCol;
            }
            return original;
        }
        if (original.startsWith(KEY_AFTER)) {
            String trueCol = original.substring(6);
            String targetCol = mappings.get(trueCol);
            if (targetCol != null) {
                return KEY_AFTER + targetCol;
            }
            return original;
        }
        return MapUtils.getString(mappings, original, original);
    }
}
