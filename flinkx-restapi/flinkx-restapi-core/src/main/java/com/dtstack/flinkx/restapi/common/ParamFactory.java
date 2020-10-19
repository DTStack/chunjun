/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.restapi.common;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ParamFactory
 *
 * @author by dujie@dtstack.com
 * @Date 2020/9/26
 */
public class ParamFactory {

    public static Pattern valueExpression =
            Pattern.compile("(?<varible>(\\$\\{(.*?)\\}))");

    public static List<ParamDefinition> createDefinition(ParamType paramType, Map<String, Map<String, String>> variableDescriptions, RestContext context) {

        //2 如果是动态变量 还需要解析为后缀表达式
        //先用一个正则 将动态变量拿出来 参数拿出来
        if (variableDescriptions == null || variableDescriptions.isEmpty()) {
            return Collections.emptyList();
        }
        List<ParamDefinition> data = new ArrayList<>(variableDescriptions.size());

        Iterator<Map.Entry<String, Map<String, String>>> iterator = variableDescriptions.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<String, String>> entry = iterator.next();
            String name = entry.getKey();
            Map<String, String> entryValue = entry.getValue();
            String value = entryValue.get("value");
            String next = entryValue.get("next");
            String format = entryValue.get("format");
            String type = entryValue.get("type");


            //1 含有next  2 value里含有动态函数  isDymatic();
            //valueItems
            //nextItems 如果不是空 或者 直接返回动态的
            if (isDymatic(value) || StringUtils.isNotBlank(next)) {
                DymaticParam dymaticParam = new DymaticParam(name, paramType, null, value, next, variableDescriptions.toString(), format, context);
                data.add(dymaticParam);
            } else {
                data.add(new ConstantParam(name, paramType, null, value, variableDescriptions.toString(), format));
            }
        }
        return data;
    }


    public static List<Paramitem> getVarible(String text) {
        List<Paramitem> valueItems = new ArrayList<>(16);
        Matcher matcher = valueExpression.matcher(text);
        while (matcher.find()) {
            String varible = matcher.group("varible");
            valueItems.add(parsr(varible));
        }
        if (CollectionUtils.isEmpty(valueItems)) {
            return Collections.emptyList();
        }
        return valueItems;
    }

    public static boolean isDymatic(String text) {
        return valueExpression.matcher(text).find();
    }


    public static Paramitem parsr(String value) {

        if (value.startsWith("${") && value.endsWith("}")) {
            String substring = value.substring(2, value.length() - 1);
            if (substring.startsWith("body.") || substring.startsWith("param.") || substring.startsWith("header.") || substring.startsWith("response.")) {
                return new ReplaceParamItem(substring);
            } else if (InnerVaribleFactory.isInnerVariable(substring)) {
                return InnerVaribleFactory.createInnerVarible(substring);
            } else {
                return new ConstantVarible(value, value);
            }
        } else {
            return new ConstantVarible(value, value);
        }
    }
}
