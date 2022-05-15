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
package com.dtstack.chunjun.conf;

import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Date: 2021/04/06 Company: www.dtstack.com
 *
 * @author tudou
 */
public class FieldConf implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 字段名称 */
    private String name;
    /** 字段类型 */
    private String type;
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
    /** 字段长度 */
    private Integer length;

    /**
     * 获取fieldList
     *
     * @param fieldList
     * @return
     */
    public static List<FieldConf> getFieldList(List fieldList) {
        List<FieldConf> list;
        if (CollectionUtils.isNotEmpty(fieldList)) {
            list = new ArrayList<>(fieldList.size());
            if (fieldList.get(0) instanceof Map) {
                for (int i = 0; i < fieldList.size(); i++) {
                    Map map = (Map) fieldList.get(i);
                    list.add(getField(map, i));
                }
            } else if (fieldList.get(0) instanceof String) {
                if (fieldList.size() == 1 && ConstantValue.STAR_SYMBOL.equals(fieldList.get(0))) {
                    FieldConf field = new FieldConf();
                    field.setName(ConstantValue.STAR_SYMBOL);
                    field.setIndex(0);
                    list.add(field);
                } else {
                    for (int i = 0; i < fieldList.size(); i++) {
                        FieldConf field = new FieldConf();
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

    /**
     * 构造field
     *
     * @param map 属性map
     * @param index 字段索引
     * @return
     */
    public static FieldConf getField(Map map, int index) {
        FieldConf field = new FieldConf();

        Object name = map.get("name");
        field.setName(name != null ? String.valueOf(name) : null);

        Object type = map.get("type");
        field.setType(type != null ? String.valueOf(type) : null);

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
        field.setPart(isPart != null ? (Boolean) isPart : false);

        Object notNull = map.get("notNull");
        field.setNotNull(notNull != null ? (Boolean) notNull : false);

        Object length = map.get("length");
        field.setLength(length != null ? (Integer) length : null);

        return field;
    }

    /**
     * 根据name查找对应FieldConf
     *
     * @param fieldList
     * @param name
     * @return
     */
    public static FieldConf getSameNameMetaColumn(List<FieldConf> fieldList, String name) {
        for (FieldConf field : fieldList) {
            if (StringUtils.isNotEmpty(field.getName()) && field.getName().equals(name)) {
                return field;
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getSplitter() {
        return splitter;
    }

    public void setSplitter(String splitter) {
        this.splitter = splitter;
    }

    public Boolean getPart() {
        return isPart;
    }

    public void setPart(Boolean part) {
        isPart = part;
    }

    public Boolean getNotNull() {
        return notNull;
    }

    public void setNotNull(Boolean notNull) {
        this.notNull = notNull;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public String getParseFormat() {
        return parseFormat;
    }

    public void setParseFormat(String parseFormat) {
        this.parseFormat = parseFormat;
    }

    @Override
    public String toString() {
        return "FieldConf{"
                + "name='"
                + name
                + '\''
                + ", type='"
                + type
                + '\''
                + ", index="
                + index
                + ", value='"
                + value
                + '\''
                + ", format='"
                + format
                + '\''
                + ", parseFormat='"
                + parseFormat
                + '\''
                + ", splitter='"
                + splitter
                + '\''
                + ", isPart="
                + isPart
                + ", notNull="
                + notNull
                + ", length="
                + length
                + '}';
    }
}
