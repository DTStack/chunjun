/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dtstack.flinkx.mapping;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2021/12/15
 */
public class NameMappingConf implements Serializable {

    private static final long serialVersionUID = 1L;

    private Map<String, Object> tableMappings = new HashMap<>();
    private Map<String, String> schemaMappings = new HashMap<>();
    private Map<String, Object> fieldMappings = new HashMap<>();
    /** 用户自定义的正则 */
    private String pattern;

    private Boolean sqlConevnt;

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public Map<String, String> getSchemaMappings() {
        return schemaMappings;
    }

    public void setSchemaMappings(Map<String, String> schemaMappings) {
        this.schemaMappings = schemaMappings;
    }

    public Map<String, Object> getTableMappings() {
        return tableMappings;
    }

    public void setTableMappings(Map<String, Object> tableMappings) {
        this.tableMappings = tableMappings;
    }

    public Map<String, Object> getFieldMappings() {
        return fieldMappings;
    }

    public void setFieldMappings(Map<String, Object> fieldMappings) {
        this.fieldMappings = fieldMappings;
    }

    public Boolean getSqlConevnt() {
        return sqlConevnt;
    }

    public void setSqlConevnt(Boolean sqlConevnt) {
        this.sqlConevnt = sqlConevnt;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", NameMappingConf.class.getSimpleName() + "[", "]")
                .add("tableMappings=" + tableMappings)
                .add("schemaMappings=" + schemaMappings)
                .add("fieldMappings=" + fieldMappings)
                .add("pattern='" + pattern + "'")
                .add("sqlConevnt='" + sqlConevnt + "'")
                .toString();
    }
}
