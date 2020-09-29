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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
            Pattern.compile("(?<constant>(.*?))(?<varible>(\\$\\{(.*?)\\}))");

    public static List<ParamDefinition> createDefinition(ParamType paramType, Map<String, Map<String, String>> variableDescriptions, RestContext context) {

        //2 如果是动态变量 还需要解析为后缀表达式
        //先用一个正则 将动态变量拿出来 参数拿出来
        if (variableDescriptions.isEmpty()) {
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
                DymaticParam dymaticParam = new DymaticParam(name, paramType, null, variableDescriptions.toString(), format, context);
                List<Paramitem> valueItems = parse(value, dymaticParam, context);
                List<Paramitem> nextItems = parse(next, dymaticParam, context);
                dymaticParam.setNextvalueDymaticParam(new ParamItemContext(nextItems, dymaticParam, context));
                dymaticParam.setValueDymaticParam(new ParamItemContext(valueItems, dymaticParam, context));
                data.add(dymaticParam);
            } else {
                data.add(new ConstantParam(name, paramType, null, value, variableDescriptions.toString(), format));
            }
        }
        return data;
    }


    private static List<Paramitem> parse(String text, ParamDefinition paramDefinition, RestContext context) {
        List<Paramitem> valueItems = new ArrayList<>(12);
        Matcher matcher = valueExpression.matcher(text);
        String lastVarible = "";
        while (matcher.find()) {
            String constant = matcher.group("constant");
            //todo
            if (!constant.equals("")) {
                valueItems.add(new ConstantVarible(constant));
            }
            String varible = matcher.group("varible");
            valueItems.add(ParamParse.parsr(varible, context, paramDefinition));
            lastVarible = varible;
        }
        //匹配到的最后一个varible的后面是匹配不到的
        if (CollectionUtils.isNotEmpty(valueItems)) {
            if (text.lastIndexOf(lastVarible) + lastVarible.length() < text.length()) {
                valueItems.add(new ConstantVarible(text.substring(text.lastIndexOf(lastVarible) + lastVarible.length())));
            }
        }
        return valueItems;
    }

    public static boolean isDymatic(String text) {
        return valueExpression.matcher(text).find();
    }

}
