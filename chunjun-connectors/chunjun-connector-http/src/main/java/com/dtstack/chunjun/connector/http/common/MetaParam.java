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

package com.dtstack.chunjun.connector.http.common;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

/** 原始header param body配置信息 */
@NoArgsConstructor
@Data
public class MetaParam implements Serializable {

    private static final long serialVersionUID = -6189228623981302042L;
    private String key;
    private String value;
    private String nextValue;
    private SimpleDateFormat timeFormat;
    private String format;
    /** 是否是嵌套key */
    private Boolean isNest = false;

    private ParamType paramType;

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
     * metaParam设置各自类型
     *
     * @param params 参数
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
            params.forEach(
                    i -> {
                        if (StringUtils.isNotBlank(i.getFormat())) {
                            i.setTimeFormat(new SimpleDateFormat(i.getFormat()));
                        }
                    });
        }
    }

    /** 获取一个metaParam的唯一名称 如body里的time参数 其全名称为body.name，因为name可能在body header里都有 */
    public String getAllName() {
        switch (paramType) {
            case PARAM:
                return ParamType.PARAM.name().toLowerCase(Locale.ENGLISH)
                        + ConstantValue.POINT_SYMBOL
                        + getKey();
            case BODY:
                return ParamType.BODY.name().toLowerCase(Locale.ENGLISH)
                        + ConstantValue.POINT_SYMBOL
                        + getKey();
            case HEADER:
                return ParamType.HEADER.name().toLowerCase(Locale.ENGLISH)
                        + ConstantValue.POINT_SYMBOL
                        + getKey();
            case RESPONSE:
                return ParamType.RESPONSE.name().toLowerCase(Locale.ENGLISH)
                        + ConstantValue.POINT_SYMBOL
                        + getKey();
            default:
                return getKey();
        }
    }

    /**
     * 获取这个metaParam的变量名 如body里的参数name 其变量名为 ${body.name}
     *
     * @return metaParam的变量名
     */
    public String getVariableName() {
        return ConstantValue.PREFIX + getAllName() + ConstantValue.SUFFIX;
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
}
