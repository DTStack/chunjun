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
package com.dtstack.chunjun.config;

import com.dtstack.chunjun.constants.ConfigConstant;
import com.dtstack.chunjun.util.GsonUtil;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Getter
@Setter
@ToString(doNotUseGetters = true)
public class OperatorConfig implements Serializable {

    private static final long serialVersionUID = 3226232793890188454L;

    /** source插件名称 */
    private String name;

    /** source配置 */
    private Map<String, Object> parameter;

    /** table设置 */
    private TableConfig table;

    /** fieldList */
    private List<FieldConfig> fieldList;

    /** fieldNameList */
    private List<String> fieldNameList;

    private String semantic;

    public List<FieldConfig> getFieldList() {
        if (fieldList == null) {
            List list = (List) parameter.get(ConfigConstant.KEY_COLUMN);
            fieldList = FieldConfig.getFieldList(list);
            fieldNameList = new ArrayList<>(fieldList.size());
            for (FieldConfig field : fieldList) {
                fieldNameList.add(field.getName());
            }
        }
        return fieldList;
    }

    public int getIntVal(String key, int defaultValue) {
        Object ret = parameter.get(key);
        if (ret == null) {
            return defaultValue;
        }
        if (ret instanceof Integer) {
            return (Integer) ret;
        }
        if (ret instanceof String) {
            return Integer.parseInt((String) ret);
        }
        if (ret instanceof Long) {
            return ((Long) ret).intValue();
        }
        if (ret instanceof Float) {
            return ((Float) ret).intValue();
        }
        if (ret instanceof Double) {
            return ((Double) ret).intValue();
        }
        if (ret instanceof BigInteger) {
            return ((BigInteger) ret).intValue();
        }
        if (ret instanceof BigDecimal) {
            return ((BigDecimal) ret).intValue();
        }
        throw new RuntimeException(
                String.format(
                        "can't %s from %s to int, internalMap = %s",
                        key, ret.getClass().getName(), GsonUtil.GSON.toJson(parameter)));
    }

    /**
     * 从parameter中获取指定key的value，若无则返回defaultValue
     *
     * @param key
     * @param defaultValue
     * @return
     */
    public long getLongVal(String key, long defaultValue) {
        Object ret = parameter.get(key);
        if (ret == null) {
            return defaultValue;
        }
        if (ret instanceof Integer) {
            return ((Integer) ret).longValue();
        }
        if (ret instanceof String) {
            return Long.parseLong((String) ret);
        }
        if (ret instanceof Long) {
            return (Long) ret;
        }
        if (ret instanceof Float) {
            return ((Float) ret).longValue();
        }
        if (ret instanceof Double) {
            return ((Double) ret).longValue();
        }
        if (ret instanceof BigInteger) {
            return ((BigInteger) ret).longValue();
        }
        if (ret instanceof BigDecimal) {
            return ((BigDecimal) ret).longValue();
        }
        throw new RuntimeException(
                String.format(
                        "can't %s from %s to long, internalMap = %s",
                        key, ret.getClass().getName(), GsonUtil.GSON.toJson(parameter)));
    }

    public String getStringVal(String key) {
        return (String) parameter.get(key);
    }

    public String getStringVal(String key, String defaultValue) {
        String ret = getStringVal(key);
        if (ret == null || ret.trim().length() == 0) {
            return defaultValue;
        }
        return ret;
    }

    public boolean getBooleanVal(String key, boolean defaultValue) {
        Object ret = parameter.get(key);
        if (ret == null) {
            return defaultValue;
        }
        if (ret instanceof Boolean) {
            return (Boolean) ret;
        }
        throw new RuntimeException(
                String.format(
                        "can't %s from %s to boolean, internalMap = %s",
                        key, ret.getClass().getName(), GsonUtil.GSON.toJson(parameter)));
    }

    @SuppressWarnings("unchecked")
    public Properties getProperties(String key, Properties p) {
        Object ret = parameter.get(key);
        if (p == null) {
            p = new Properties();
        }
        if (ret == null) {
            return p;
        }
        if (ret instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) ret;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                p.setProperty(entry.getKey(), String.valueOf(entry.getValue()));
            }
            return p;
        } else {
            throw new RuntimeException(
                    String.format(
                            "can't %s from %s to map, internalMap = %s",
                            key, ret.getClass().getName(), GsonUtil.GSON.toJson(parameter)));
        }
    }
}
