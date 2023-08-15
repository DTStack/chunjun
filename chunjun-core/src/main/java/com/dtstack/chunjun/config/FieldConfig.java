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

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.util.GsonUtil;

import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Data
public class FieldConfig implements Serializable {

    private static final long serialVersionUID = -5874873362857155703L;

    /** 字段名称 */
    private String name;
    /** 字段类型 */
    private TypeConfig scriptType;
    /** 字段索引 */
    private Integer index;
    /** 字段常量值 */
    private String value;
    /** 如果字段是时间字符串，可以指定时间的格式，将字段类型转为日期格式返回 */
    private String format;
    /** 与format字段组合使用，用于时间字符串格式转换 parseFormat->时间戳->format */
    private String parseFormat;
    /** 字段分隔符 */
    private String splitter;
    /** 是否为分区字段 */
    private Boolean isPart = false;
    /** 字段是否可空 */
    private Boolean notNull = false;

    public static List<FieldConfig> getFieldList(List fieldList) {
        List<FieldConfig> list;
        if (CollectionUtils.isNotEmpty(fieldList)) {
            list = new ArrayList<>(fieldList.size());
            if (fieldList.get(0) instanceof Map) {
                for (int i = 0; i < fieldList.size(); i++) {
                    Map map = (Map) fieldList.get(i);
                    list.add(getField(map, i));
                }
            } else if (fieldList.get(0) instanceof String) {
                if (fieldList.size() == 1 && ConstantValue.STAR_SYMBOL.equals(fieldList.get(0))) {
                    FieldConfig field = new FieldConfig();
                    field.setName(ConstantValue.STAR_SYMBOL);
                    field.setIndex(0);
                    list.add(field);
                } else {
                    for (int i = 0; i < fieldList.size(); i++) {
                        FieldConfig field = new FieldConfig();
                        field.setName(String.valueOf(fieldList.get(i)));
                        field.setIndex(i);
                        list.add(field);
                    }
                }
            } else {
                throw new IllegalArgumentException(
                        "column argument error, fieldList = " + GsonUtil.GSON.toJson(fieldList));
            }
        } else {
            list = Collections.emptyList();
        }
        return list;
    }

    public TypeConfig getType() {
        return scriptType;
    }

    public void setType(TypeConfig type) {
        this.scriptType = type;
    }

    public static FieldConfig getField(Map map, int index) {
        FieldConfig field = new FieldConfig();

        Object name = map.get("name");
        field.setName(name != null ? String.valueOf(name) : null);

        Object type = map.get("type");
        field.setScriptType(type == null ? null : TypeConfig.fromString(String.valueOf(type)));

        Object colIndex = map.get("index");
        if (Objects.nonNull(colIndex)) {
            field.setIndex((Integer) colIndex);
        } else {
            field.setIndex(index);
        }

        Object value = map.get("value");
        field.setValue(value != null ? String.valueOf(value) : null);

        Object format = map.get("format");
        if (format != null && String.valueOf(format).trim().length() > 0) {
            field.setFormat(String.valueOf(format));
        }

        Object parseFormat = map.get("parseFormat");
        if (parseFormat != null && String.valueOf(parseFormat).trim().length() > 0) {
            field.setParseFormat(String.valueOf(parseFormat));
        }

        Object splitter = map.get("splitter");
        field.setSplitter(splitter != null ? String.valueOf(splitter) : null);

        Object isPart = map.get("isPart");
        field.setIsPart(isPart != null ? (Boolean) isPart : false);

        Object notNull = map.get("notNull");
        field.setNotNull(notNull != null ? (Boolean) notNull : false);

        return field;
    }

    public static FieldConfig getSameNameMetaColumn(List<FieldConfig> fieldList, String name) {
        for (FieldConfig field : fieldList) {
            if (StringUtils.isNotEmpty(field.getName()) && field.getName().equals(name)) {
                return field;
            }
        }
        return null;
    }
}
