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

package com.dtstack.flinkx.restapi.common;

import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.MapUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * 原始header param body配置信息
 *
 * @author dujie
 */
public class MetaParam implements Serializable {

    private String key;
    private String value;
    private String nextValue;
    private SimpleDateFormat timeFormat;
    private String format;
    /**
     * 是否是嵌套key
     **/
    private Boolean isNest = false;
    private ParamType paramType;


    public MetaParam() {
    }

    public MetaParam(String key, String value, ParamType paramType) {
        this.key = key;
        this.value = value;
        this.paramType = paramType;
    }

    public MetaParam(String key, String value, ParamType paramType, boolean isNest) {
        this.key = key;
        this.value = value;
        this.paramType = paramType;
        this.isNest = isNest;
    }

    /**
     * metaparam设置各自类型
     *
     * @param params    参数
     * @param paramType 类型
     */
    public static void setMetaColumnsType(List<MetaParam> params, ParamType paramType) {

        if (CollectionUtils.isNotEmpty(params)) {
            params.forEach(i -> i.setParamType(paramType));
        }
    }

    /**
     * 将json脚本里的format转为SimpleDateFormat
     *
     * @param params json配置信息
     */
    public static void initTimeFormat(List<MetaParam> params) {

        if (CollectionUtils.isNotEmpty(params)) {
            params.forEach(i -> {
                if (StringUtils.isNotBlank(i.getFormat())) {
                    i.setTimeFormat(new SimpleDateFormat(i.getFormat()));
                }
            });
        }
    }


    /**
     * 获取一个metaparam的唯一名称 如body里的time参数 其全名称为body.name，因为name可能在body header里都有
     */
    public String getAllName() {
        switch (paramType) {
            case PARAM:
                return ParamType.PARAM.name().toLowerCase(Locale.ENGLISH) + ConstantValue.POINT_SYMBOL + getKey();
            case BODY:
                return ParamType.BODY.name().toLowerCase(Locale.ENGLISH) + ConstantValue.POINT_SYMBOL + getKey();
            case HEADER:
                return ParamType.HEADER.name().toLowerCase(Locale.ENGLISH) + ConstantValue.POINT_SYMBOL + getKey();
            case RESPONSE:
                return ParamType.RESPONSE.name().toLowerCase(Locale.ENGLISH) + ConstantValue.POINT_SYMBOL + getKey();
            default:
                return getKey();
        }
    }


    /**
     * 获取这个metaparam的变量名 如body里的参数name 其变量名为 ${body.name}
     *
     * @return metaparam的变量名
     */
    public String getVariableName() {
        return new StringBuilder().append(com.dtstack.flinkx.restapi.common.ConstantValue.PREFIX).append(getAllName()).append(com.dtstack.flinkx.restapi.common.ConstantValue.SUFFIX).toString();
    }


    /**
     * 根据是否是第一次 获取真正的表达式
     *
     * @param isFirst 是否是第一次请求
     * @return 参数对应的表达式
     */
    public String getActualValue(boolean isFirst) {
        if (StringUtils.isEmpty(nextValue)) {
            return value;
        }
        return isFirst ? value : nextValue;
    }

    public String getNextValue() {
        return nextValue;
    }

    public void setNextValue(String nextValue) {
        this.nextValue = nextValue;
    }


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public SimpleDateFormat getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(SimpleDateFormat timeFormat) {
        this.timeFormat = timeFormat;
    }


    public ParamType getParamType() {
        return paramType;
    }

    public void setParamType(ParamType paramType) {
        this.paramType = paramType;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Boolean getNest() {
        return isNest;
    }

    public void setNest(Boolean nest) {
        isNest = nest;
    }

    @Override
    public String toString() {
        return "MetaParam{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", nextValue='" + nextValue + '\'' +
                ", timeFormat=" + timeFormat +
                ", isNest=" + isNest +
                ", format='" + format + '\'' +
                ", paramType=" + paramType +
                '}';
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + (StringUtils.isBlank(key) ? 0 : key.hashCode());
        result = 31 * result + (StringUtils.isBlank(value) ? 0 : value.hashCode());
        result = 31 * result + (StringUtils.isBlank(nextValue) ? 0 : nextValue.hashCode());
        result = 31 * result + (isNest == null ? 0 : isNest.hashCode());
        result = 31 * result + (StringUtils.isBlank(format) ? 0 : format.hashCode());
        result = 31 * result + (paramType == null ? 0 : paramType.hashCode());
        return result;
    }
}
