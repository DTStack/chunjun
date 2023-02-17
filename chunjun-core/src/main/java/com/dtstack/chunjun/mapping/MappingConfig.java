/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.dtstack.chunjun.mapping;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

public class MappingConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * if the upstream is Kafka, you need to specify the upstream data type with this parameter to
     * get the corresponding DDL converter. *
     */
    private String sourceName;

    private final String casing = Casing.UNCHANGE.name();

    private final Map<String, String> identifierMappings;
    private final Map<String, String> columnTypeMappings;

    public MappingConfig(
            LinkedHashMap<String, String> identifierMappings,
            LinkedHashMap<String, String> columnTypeMappings) {
        this.identifierMappings = identifierMappings;
        this.columnTypeMappings = columnTypeMappings;
    }

    public Map<String, String> getIdentifierMappings() {
        return identifierMappings;
    }

    public Map<String, String> getColumnTypeMappings() {
        return columnTypeMappings;
    }

    public boolean isEmpty() {
        return identifierMappings.isEmpty() && columnTypeMappings.isEmpty();
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getCasing() {
        return casing;
    }

    public Boolean needReplaceMetaData() {
        return MapUtils.isNotEmpty(identifierMappings)
                || MapUtils.isNotEmpty(columnTypeMappings)
                || (StringUtils.isNotBlank(casing) && !Casing.UNCHANGE.name().equals(casing));
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MappingConfig.class.getSimpleName() + "[", "]")
                .add("sourceName='" + sourceName + "'")
                .add("casing='" + casing + "'")
                .add("identifierMappings=" + identifierMappings)
                .add("columnTypeMappings=" + columnTypeMappings)
                .toString();
    }
}
