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
import com.dtstack.flinkx.util.DateUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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

    public static List<MetaParam> getMetaColumns(List params, ParamType paramType) {

        if (CollectionUtils.isNotEmpty(params)) {
            ArrayList<MetaParam> metaParams = new ArrayList<>();
            for (int i = 0; i < params.size(); i++) {
                Map sm = (Map) params.get(i);

                MetaParam mc = new MetaParam();
                mc.setParamType(paramType);
                mc.setName(String.valueOf(sm.get("name")));
                mc.setValue(String.valueOf(sm.get("value")));
                //header是没有nextValue的
                if(!paramType.equals(ParamType.HEADER)){
                    mc.setNextValue(sm.get("nextValue") != null ? String.valueOf(sm.get("nextValue")) : null);
                }
                if (sm.get("format") != null && String.valueOf(sm.get("format")).trim().length() > 0) {
                    mc.setTimeFormat(DateUtil.buildDateFormatter(String.valueOf(sm.get("format")).trim()));
                }
                metaParams.add(mc);
            }
            return metaParams;
        }

        return Collections.emptyList();
    }


    public String getNextValue() {
        return nextValue;
    }

    public String getActualValue(boolean isFirst) {

        if(StringUtils.isEmpty(nextValue)){
            return value;
        }
        return isFirst ? value : nextValue;
    }

    public void setNextValue(String nextValue) {
        this.nextValue = nextValue;
    }

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


    public String getVariableName(){
        return new StringBuilder().append(com.dtstack.flinkx.restapi.common.ConstantValue.PREFIX).append(getAllName()).append(com.dtstack.flinkx.restapi.common.ConstantValue.SUFFIX).toString();
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
