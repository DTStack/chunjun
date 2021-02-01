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

package com.dtstack.flinkx.restapi.inputformat;

import com.dtstack.flinkx.restapi.common.ConstantValue;
import com.dtstack.flinkx.restapi.common.HttpUtil;
import com.dtstack.flinkx.restapi.common.MetaParam;
import com.dtstack.flinkx.restapi.common.ParamType;
import com.dtstack.flinkx.restapi.reader.HttpRestConfig;
import com.dtstack.flinkx.restapi.reader.Strategy;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DefaultRestHandler implements RestHandler {
    private Gson gson = new Gson();

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRestHandler.class);

    // 这个正则为匹配表达式中的数字或运算符
    public static Pattern p = Pattern.compile("(?<!\\d)-?\\d+(\\.\\d+)?|[+\\-]");


    @Override
    public Strategy chooseStrategy(List<Strategy> strategies, Map<String, Object> responseValue, HttpRestConfig restConfig, HttpRequestParam httpRequestParam) {
        //根据指定的key  获取指定的值

        return strategies.stream().filter(i -> {
            MetaParam metaParam = new MetaParam();
            if (!MetaparamUtils.isDynamic(i.getKey())) {
                throw new IllegalArgumentException("strategy key " + i.getKey() + " is error,wo just support ${response.},${param.},${body.}");
            }

            metaParam.setValue(i.getKey());
            metaParam.setName(i.getKey());
            metaParam.setParamType(ParamType.BODY);

            Object object = getValue(metaParam, null, null, responseValue, restConfig, true, httpRequestParam);

            String value = i.getValue();
            if (MetaparamUtils.isDynamic(i.getValue())) {
                metaParam.setName(i.getKey() + UUID.randomUUID().toString());
                metaParam.setValue(i.getValue());
                value = getValue(metaParam, null, null, responseValue, restConfig, true, httpRequestParam).toString();
            }
            if (object.toString().equals(value)) {
                LOG.info("select a Strategy, key {}  value is {} ,key {} value is {} ,responseValue is {} ,httpRequestParam {}", i.getKey(), object.toString(), i.getValue(), value, GsonUtil.GSON.toJson(responseValue), httpRequestParam.toString());
                return true;
            } else {
                return false;
            }


        }).findFirst().orElse(null);

    }

    @Override
    public HttpRequestParam buildRequestParam(List<MetaParam> metaParams, List<MetaParam> metaHeaders, HttpRequestParam prevRequestParam, Map<String, Object> prevResponseValue, HttpRestConfig restConfig, boolean first) {
        HttpRequestParam requestParam = new HttpRequestParam();
        init(metaParams, metaHeaders, prevRequestParam, prevResponseValue, restConfig, first, requestParam);
        return requestParam;
    }

    @Override
    public ResponseValue buildData(String decode, String responseValue, String fields) {
        if (decode.equals(ConstantValue.DEFAULT_DECODE)) {
            Map map = HttpUtil.gson.fromJson(responseValue, Map.class);
            if (StringUtils.isEmpty(fields)) {
                return new ResponseValue(gson.toJson(map));
            } else {
                return new ResponseValue(gson.toJson(buildResponseByKey(map, Arrays.asList(fields.split(",")))));
            }
        } else {
            return new ResponseValue(responseValue);
        }
    }

    public Object getResponseValue(Map<String, Object> map, String key) {
        Object o = null;
        String[] split = key.split("\\.");
        Map<String, Object> stringObjectHashMap = map;
        for (int i = 0; i < split.length; i++) {
            o = getValue(stringObjectHashMap, split[i]);
            if (o == null) {
                throw new RuntimeException(key + " not exist on response [" + GsonUtil.GSON.toJson(map) + "] ");
            }
            if (i != split.length - 1) {
                if (!(o instanceof Map)) {
                    throw new RuntimeException("key " + key + " in " + map + " is not a json");
                }
                stringObjectHashMap = (Map<String, Object>) o;
            }
        }
        return o;
    }

    public Map buildResponseByKey(Map<String, Object> map, List<String> fields) {
        HashMap<String, Object> value = new HashMap<>();

        for (String key : fields) {
            value.put(key, getResponseValue(map, key));
        }

        HashMap<String, Object> stringObjectHashMap = new HashMap<>();
        value.forEach((k, v) -> {
            String[] split = k.split("\\.");
            if (split.length == 1) {
                stringObjectHashMap.put(split[0], v);
            } else {
                HashMap<String, Object> temp = stringObjectHashMap;

                for (int i = 0; i < split.length - 1; i++) {

                    if (temp.containsKey(split[i])) {
                        temp = (HashMap) temp.get(split[i]);
                    } else {
                        HashMap<String, Object> hashMap = new HashMap(2);
                        temp.put(split[i], hashMap);
                        temp = hashMap;
                    }

                    if (i == split.length - 2) {
                        temp.put(split[split.length - 1], v);
                    }
                }
            }
        });
        return stringObjectHashMap;
    }

    public static Object getValue(Map<String, Object> map, String key) {
        if (!map.containsKey(key)) {
            throw new RuntimeException(key + " not exist on response [" + GsonUtil.GSON.toJson(map) + "] ");
        }
        return map.get(key);
    }


    public void init(List<MetaParam> originalParam, List<MetaParam> originalHeader, HttpRequestParam prevRequestParam, Map<String, Object> prevResponseValue, HttpRestConfig restConfig, boolean first, HttpRequestParam currentParam) {

        ArrayList<MetaParam> metaParams = new ArrayList<>(originalParam.size() + originalHeader.size());
        metaParams.addAll(originalParam);
        metaParams.addAll(originalHeader);

        Map<String, MetaParam> allParam = metaParams.stream().collect(Collectors.toMap(MetaParam::getAllName, Function.identity()));

        metaParams.forEach(i -> getValue(i, allParam, prevRequestParam, prevResponseValue, restConfig, first, currentParam));
    }

    public Object getValue(MetaParam metaParam, Map<String, MetaParam> map, HttpRequestParam prevRequestParam, Map<String, Object> prevResponseValue, HttpRestConfig restConfig, boolean first, HttpRequestParam currentParam) {

        String value = metaParam.getValue();
        if (!first && StringUtils.isNotBlank(metaParam.getNextValue())) {
            value = metaParam.getNextValue();
        }

        String actualValue;

        if (currentParam.containsParam(metaParam.getName())) {
            return currentParam.getValue(metaParam.getName());
        }

        List<Map<String, Pair<MetaParam, Object>>> variableValues = new ArrayList<>(8);


        List<MetaParam> metaParams = MetaparamUtils.getValueOfMetaParams(value, restConfig, map);

        if (CollectionUtils.isNotEmpty(metaParams)) {
            metaParams.forEach(i -> {
                if (i.getParamType().equals(ParamType.INNER)) {
                    variableValues.add(Collections.singletonMap(i.getVariableName(), Pair.of(i, i.getValue())));
                } else if (i.getParamType().equals(ParamType.RESPONSE)) {
                    //根据 返回值获取对应的值
                    String variableValue = getResponseValue(prevResponseValue, i.getName()).toString();
                    if (Objects.isNull(variableValue)) {
                        throw new RuntimeException(metaParam.getName() + " not exist on response [" + prevResponseValue + "] ");
                    }

                    variableValues.add(Collections.singletonMap(i.getVariableName(), Pair.of(i, variableValue)));
                } else {
                    String variableValue;
                    if (i.getName().equals(metaParam.getName())) {
                        variableValue = prevRequestParam.getValue(i.getName()).toString();
                    } else {
                        variableValue = getValue(i, map, prevRequestParam, prevResponseValue, restConfig, first, currentParam).toString();
                    }
                    variableValues.add(Collections.singletonMap(i.getVariableName(), Pair.of(i, variableValue)));
                }

            });

            actualValue = calculateValue(metaParam, variableValues, first);

        } else {
            actualValue = getData(value, metaParam);
        }

        currentParam.putValue(metaParam.getParamType(), metaParam.getName(), actualValue);
        return actualValue;
    }

    //Pair
    private BigDecimal getNumber(MetaParam metaParam, List<Map<String, Pair<MetaParam, Object>>> variableValues, boolean first) {

        if (variableValues.size() > 2) {
            return null;
        }


        AtomicReference<String> value = new AtomicReference<>(metaParam.getValue());
        if (!first && StringUtils.isNotBlank(metaParam.getNextValue())) {
            value.set(metaParam.getNextValue());
        }


        if (variableValues.size() == 1) {
            HashSet<Character> characters = Sets.newHashSet('+', '-');
            String key = variableValues.get(0).keySet().iterator().next();
            if (value.get().startsWith(key) && !characters.contains(value.get().charAt(key.length()))) {
                return null;
            } else if (value.get().endsWith(key) && !characters.contains(value.get().charAt(value.get().length() - key.length() - 1))) {
                return null;
            }
        } else if (variableValues.size() == 2) {
            List<String> strings = variableValues.stream().map(i -> i.keySet().iterator().next()).collect(Collectors.toList());
            if (!Sets.newHashSet(strings.get(0) + "+" + strings.get(1), strings.get(0) + "-" + strings.get(1)).contains(value.get())) {
                return null;
            }
        } else {
            return null;
        }

        try {
            BigDecimal decimal;
            for (Map<String, Pair<MetaParam, Object>> variableValue : variableValues) {
                for (Map.Entry<String, Pair<MetaParam, Object>> next : variableValue.entrySet()) {
                    String key = next.getKey();
                    Pair<MetaParam, Object> pair = next.getValue();
                    MetaParam left = pair.getLeft();
                    String right = pair.getRight().toString();

                    if (NumberUtils.isNumber(right)) {
                        value.set(value.get().replaceFirst(escapeExprSpecialWord(key), NumberUtils.createBigDecimal(right).toString()));
                    } else {

                        if (StringUtils.isEmpty(right)) {
                            return null;
                        }

                        //todo 先是metaparam解析  然后是 left的去解析 然后是 默认的去解析
                        if (Objects.nonNull(metaParam.getTimeFormat())) {
                            try {
                                value.set(value.get().replaceFirst(escapeExprSpecialWord(key), left.getTimeFormat().parse(right).getTime() + ""));
                            } catch (ParseException e) {
                                try {
                                    value.set(value.get().replaceFirst(escapeExprSpecialWord(key), metaParam.getTimeFormat().parse(right).getTime() + ""));
                                } catch (ParseException e1) {
                                    try {
                                        value.set(value.get().replaceFirst(escapeExprSpecialWord(key), DateUtil.stringToDate(right, null).getTime() + ""));
                                    } catch (Exception e2) {
                                        //如果默认的解析不掉
                                        return null;
                                    }
                                }
                            }
                        }
                    }

                }
            }

            String s = value.get();

            //判断是否只有+-以及数字
            if (!p.matcher(s).find()) {
                return null;
            }

            String a;
            String b;
            String operation;

            if (s.contains("+")) {
                operation = "+";
                int i = StringUtils.indexOfIgnoreCase(s, "+");
                a = s.substring(0, i);
                b = s.substring(i + 1);
            } else {
                operation = "-";
                int i = StringUtils.indexOfIgnoreCase(s, "-");
                a = s.substring(0, i);
                b = s.substring(i + 1);
            }
            if (NumberUtils.isNumber(a) && NumberUtils.isNumber(b)) {
                if ("+".equalsIgnoreCase(operation)) {
                    decimal = new BigDecimal(a).add(new BigDecimal(b));
                } else {
                    decimal = new BigDecimal(a).subtract(new BigDecimal(b));
                }
                return decimal;
            }

        } catch (Exception e) {
            return null;
        }
        return null;
    }

    public String calculateValue(MetaParam metaParam, List<Map<String, Pair<MetaParam, Object>>> variableValues, boolean first) {
        Object object;
        if (CollectionUtils.isNotEmpty(variableValues)) {
            if (variableValues.size() == 1 && variableValues.get(0).keySet().iterator().next().equals(metaParam.getActualValue(first))) {
                object = variableValues.get(0).get(variableValues.get(0).keySet().iterator().next()).getRight();
            } else {
                if ((object = getNumber(metaParam, variableValues, first)) == null) {
                    object = getString(metaParam, variableValues, first);
                }
            }
        } else {
            //没有变量 就只是一个简单的字符串常量
            object = metaParam.getValue();
        }
        return getData(object, metaParam);

    }


    private String getString(MetaParam metaParam, List<Map<String, Pair<MetaParam, Object>>> variableValues, boolean first) {

        AtomicReference<String> value = new AtomicReference<>();

        if (first) {
            value.set(metaParam.getValue());
        } else {
            value.set(metaParam.getNextValue());
        }

        variableValues.forEach(i -> i.forEach((k, v) -> value.set(value.get().replaceFirst(escapeExprSpecialWord(k), v.getRight().toString()))));

        return value.get();
    }


    /**
     * 如果value是动态计算并且要求输出的格式是date格式 则format是必须要设置的，
     * 否则计算出来的结果不是format格式的 而是时间戳格式
     */
    public String getData(Object object, MetaParam metaParam) {
        String data;
        String s1 = object.toString();
        //假如是 metaParam是来自于response的 只能解析默认支持的类型  如果是由其他的key如param body组成的 则取他们的format去格式化
        if (Objects.nonNull(metaParam.getTimeFormat())) {
            if (isLong(s1)) {
                data = metaParam.getTimeFormat().format(new Date(NumberUtils.createLong(s1)));
            } else {
                try {
                    data = metaParam.getTimeFormat().format(metaParam.getTimeFormat().parse(s1));
                } catch (ParseException e) {
                    throw new RuntimeException(metaParam.getTimeFormat().toPattern() + "parse data[" + s1 + "] error", e);
                }
            }
        } else {
            data = s1;
        }
        return data;
    }


    public boolean isLong(String data) {
        try {
            Long.parseLong(data);
            return true;
        } catch (Exception e) {
            return false;
        }
    }


    /**
     * 转义正则特殊字符 （$()*+.[]?\^{},|）
     */
    public static String escapeExprSpecialWord(String keyword) {
        if (StringUtils.isNotBlank(keyword)) {
            String[] fbsArr = {"\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|"};
            for (String key : fbsArr) {
                if (keyword.contains(key)) {
                    keyword = keyword.replace(key, "\\" + key);
                }
            }
        }
        return keyword;
    }


}
