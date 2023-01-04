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

package com.dtstack.chunjun.connector.http.client;

import com.dtstack.chunjun.connector.http.common.ConstantValue;
import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.connector.http.common.MetaParam;
import com.dtstack.chunjun.connector.http.common.ParamType;
import com.dtstack.chunjun.util.DateUtil;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetaparamUtils {
    public static Pattern valueExpression =
            Pattern.compile(
                    "(?<variable>(\\$\\{((?<innerName>(uuid|currentTime|intervalTime))|((?<paramType>(param|response|body))\\.(?<name>(.*?[^${]*))))}))");

    /**
     * 获取一个变量所关联的其他变量
     *
     * @param expression 解析的表达式${body.xxxx}
     * @param nest expression是否需要进行切割
     * @param restConfig http的相关配置
     * @param requestParam 所有的原始MetaParam构造结果
     */
    public static List<MetaParam> getValueOfMetaParams(
            String expression,
            Boolean nest,
            HttpRestConfig restConfig,
            HttpRequestParam requestParam) {

        Matcher matcher = valueExpression.matcher(expression);

        ArrayList<MetaParam> metaParams = new ArrayList<>(12);
        while (matcher.find()) {
            // 整个变量 如${body.time}
            String variableName = matcher.group("variable");
            // 变量的名称 如${body.time}里的time
            String key = matcher.group("name");
            // 内置变量名称 如currentTime
            String innerName = matcher.group("innerName");

            if (StringUtils.isNotBlank(innerName)) {
                MetaParam innerMetaParam = new MetaParam();
                innerMetaParam.setKey(innerName);
                innerMetaParam.setParamType(ParamType.INNER);

                switch (innerName) {
                    case ConstantValue.SYSTEM_FUNCTION_CURRENT_TIME:
                        innerMetaParam.setValue(DateUtil.timestampToString(new Date()));
                        innerMetaParam.setTimeFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
                        break;
                    case ConstantValue.SYSTEM_FUNCTION_INTERVAL_TIME:
                        innerMetaParam.setValue(restConfig.getIntervalTime() + "");
                        break;
                    case ConstantValue.SYSTEM_FUNCTION_UUID:
                        innerMetaParam.setValue(UUID.randomUUID().toString());
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "inner function is not support " + innerName);
                }

                metaParams.add(innerMetaParam);

            } else {
                ParamType variableType =
                        ParamType.valueOf(matcher.group("paramType").toUpperCase(Locale.ENGLISH));

                if (variableType.equals(ParamType.RESPONSE)) {
                    MetaParam param = new MetaParam();
                    param.setParamType(ParamType.RESPONSE);
                    param.setKey(key);
                    metaParams.add(param);
                } else {
                    Map<String, Object> map;
                    if (variableType.equals(ParamType.PARAM)) {
                        map = requestParam.getParam();
                    } else if (variableType.equals(ParamType.BODY)) {
                        map = requestParam.getBody();
                    } else {
                        map = requestParam.getHeader();
                    }

                    MetaParam param = new MetaParam();
                    param.setParamType(variableType);
                    param.setKey(key);
                    // nest为空 代表不知道表达式的key是否需要切割 有可能是异常策略转换的metaparam
                    if (null != nest) {
                        param.setIsNest(nest);
                    }

                    if (requestParam.containsKey(param, restConfig.getFieldDelimiter())) {
                        metaParams.add(
                                (MetaParam)
                                        requestParam.getValue(
                                                param, restConfig.getFieldDelimiter()));
                    } else {
                        param.setIsNest(true);
                        // 没找到 就代表动态参数指定的key不存在 直接报错
                        if (null != nest
                                || !requestParam.containsKey(
                                        param, restConfig.getFieldDelimiter())) {
                            throw new RuntimeException(
                                    "the metaParam pointed by the key ["
                                            + key
                                            + " ] of "
                                            + expression
                                            + " does not exist");
                        }
                        metaParams.add(
                                (MetaParam)
                                        requestParam.getValue(
                                                param, restConfig.getFieldDelimiter()));
                    }
                }
            }
        }
        return metaParams;
    }

    /**
     * 表达式 是否是动态变量而不是常量，包含内部变量，body response header param等 都是变量
     *
     * @param text 表达式
     */
    public static boolean isDynamic(String text) {
        return valueExpression.matcher(text).find();
    }

    public static boolean isInnerParam(String text) {
        return Sets.newHashSet(
                        ConstantValue.PREFIX
                                + ConstantValue.SYSTEM_FUNCTION_UUID
                                + ConstantValue.SUFFIX,
                        ConstantValue.PREFIX
                                + ConstantValue.SYSTEM_FUNCTION_UUID
                                + ConstantValue.SUFFIX,
                        ConstantValue.PREFIX
                                + ConstantValue.SYSTEM_FUNCTION_INTERVAL_TIME
                                + ConstantValue.SUFFIX)
                .contains(text);
    }
}
