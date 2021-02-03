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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

/**
 * 原始header param body配置信息
 *
 * @author dujie
 */
public class MetaParam implements Serializable {

    private String name;
    private String value;
    private String nextValue;
    private SimpleDateFormat timeFormat;
    private ParamType paramType;


    public MetaParam() {
    }

    public MetaParam(String name, String value, ParamType paramType) {
        this.name = name;
        this.value = value;
        this.paramType = paramType;
    }

    /**
     * metaparam设置各自类型
     * @param params 参数
     * @param paramType 类型
     */
    public static void setMetaColumnsType(List<MetaParam> params, ParamType paramType) {

        if (CollectionUtils.isNotEmpty(params)) {
            params.forEach(i -> i.setParamType(paramType));
        }
    }


    /**
     * 获取一个metaparam的唯一名称 如body里的time参数 其全名称为body.name，因为name可能在body header里都有
     */
    public String getAllName() {
        switch (paramType) {
            case PARAM:
                return ParamType.PARAM.name().toLowerCase(Locale.ENGLISH) + ConstantValue.POINT_SYMBOL + getName();
            case BODY:
                return ParamType.BODY.name().toLowerCase(Locale.ENGLISH) + ConstantValue.POINT_SYMBOL + getName();
            case HEADER:
                return ParamType.HEADER.name().toLowerCase(Locale.ENGLISH) + ConstantValue.POINT_SYMBOL + getName();
            case RESPONSE:
                return ParamType.RESPONSE.name().toLowerCase(Locale.ENGLISH) + ConstantValue.POINT_SYMBOL + getName();
            default:
                return getName();
        }
    }


    /**
     * 获取这个metaparam的变量名 如body里的参数name 其变量名为 ${body.name}
     * @return metaparam的变量名
     */
    public String getVariableName() {
        return new StringBuilder().append(com.dtstack.flinkx.restapi.common.ConstantValue.PREFIX).append(getAllName()).append(com.dtstack.flinkx.restapi.common.ConstantValue.SUFFIX).toString();
    }


    /**
     * 根据是否是第一次 获取真正的表达式
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


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    @Override
    public String toString() {
        return "MetaParam{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                ", nextValue='" + nextValue + '\'' +
                ", timeFormat=" + timeFormat +
                '}';
    }
}
