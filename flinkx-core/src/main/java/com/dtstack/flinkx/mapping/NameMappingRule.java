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

package com.dtstack.flinkx.mapping;

import java.io.Serializable;
import java.util.Map;

/**
 * @author tiezhu@dtstack.com
 * @since 2021/12/23 星期四
 */
public class NameMappingRule implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> schemaMappings;

    private final Map<String, String> tableMappings;

    private final Map<String, String> fieldMappings;

    public NameMappingRule(NameMappingConf conf) {
        this.schemaMappings = conf.getSchemaMappings();
        this.tableMappings = conf.getTableMappings();
        this.fieldMappings = conf.getFieldMappings();
    }

    public String schemaMapping(String original) {
        return mapping(original, schemaMappings);
    }

    public String tableMapping(String original) {
        return mapping(original, tableMappings);
    }

    public String fieldMapping(String original) {
        return mapping(original, fieldMappings);
    }

    private String mapping(String original, Map<String, String> mappings) {
        if (mappings.containsKey(original)) {
            return mappings.get(original);
        }

        return original;
    }
}
