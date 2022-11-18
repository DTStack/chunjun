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

import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.connector.http.common.MetaParam;
import com.dtstack.chunjun.connector.http.common.ParamType;
import com.dtstack.chunjun.util.DateUtil;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.GsonUtil;
import com.dtstack.chunjun.util.MapUtil;
import com.dtstack.chunjun.util.SnowflakeIdWorker;
import com.dtstack.chunjun.util.StringUtil;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class DefaultRestHandler implements RestHandler {
    private final SnowflakeIdWorker snowflakeIdWorker = new SnowflakeIdWorker(1, 1);

    /** match number */
    public static Pattern p = Pattern.compile("(?<!\\d)-?\\d+(\\.\\d+)?|[+\\-]");

    public static final Gson gson = GsonUtil.setTypeAdapter(new Gson());

    @Override
    public Strategy chooseStrategy(
            List<Strategy> strategies,
            Map<String, Object> responseValue,
            HttpRestConfig restConfig,
            HttpRequestParam httpRequestParam,
            List<MetaParam> metaParams) {

        // origin param converter to  HttpRequestParam
        HttpRequestParam originParam = new HttpRequestParam();
        metaParams.forEach(i -> originParam.putValue(i, restConfig.getFieldDelimiter(), i));

        return strategies.stream()
                .filter(
                        i -> {
                            MetaParam metaParam = new MetaParam();
                            // Strategy里指定的动态变量不知道是不是嵌套的
                            metaParam.setIsNest(null);
                            // key一定是一个动态变量且不是内置变量
                            if (!MetaparamUtils.isDynamic(i.getKey())
                                    && !MetaparamUtils.isInnerParam(i.getKey())) {
                                throw new IllegalArgumentException(
                                        "strategy key "
                                                + i.getKey()
                                                + " is error,we just support ${response.},${param.},${body.}");
                            }

                            HttpRequestParam copy = HttpRequestParam.copy(httpRequestParam);

                            metaParam.setKey(snowflakeIdWorker.nextId() + "");
                            metaParam.setValue(i.getKey());
                            // 指定一个类别
                            metaParam.setParamType(ParamType.BODY);

                            Object object =
                                    getValue(
                                            metaParam,
                                            originParam,
                                            null,
                                            responseValue,
                                            restConfig,
                                            true,
                                            copy);

                            String value = i.getValue();
                            // value可以是内置变量
                            if (MetaparamUtils.isDynamic(i.getValue())) {
                                metaParam.setKey(snowflakeIdWorker.nextId() + "");
                                metaParam.setValue(i.getValue());
                                value =
                                        getValue(
                                                        metaParam,
                                                        originParam,
                                                        null,
                                                        responseValue,
                                                        restConfig,
                                                        true,
                                                        copy)
                                                .toString();
                            }
                            if (object.toString().equals(value)) {
                                log.info(
                                        "select a Strategy, key {}  value is {} ,key {} value is {} ,responseValue is {} ,httpRequestParam {}",
                                        i.getKey(),
                                        object,
                                        i.getValue(),
                                        value,
                                        GsonUtil.GSON.toJson(responseValue),
                                        httpRequestParam);
                                return true;
                            } else {
                                return false;
                            }
                        })
                .findFirst()
                .orElse(null);
    }

    @Override
    public HttpRequestParam buildRequestParam(
            List<MetaParam> metaParamList,
            List<MetaParam> metaBodyList,
            List<MetaParam> metaHeaderList,
            HttpRequestParam prevRequestParam,
            Map<String, Object> prevResponseValue,
            HttpRestConfig restConfig,
            boolean first) {
        HttpRequestParam requestParam = new HttpRequestParam();
        buildParam(
                metaParamList,
                metaBodyList,
                metaHeaderList,
                prevRequestParam,
                prevResponseValue,
                restConfig,
                first,
                requestParam);
        return requestParam;
    }

    /**
     * 根据指定的key 构建一个新的response
     *
     * @param map 返回值
     * @param fields 指定字段
     */
    public Map<String, Object> buildResponseByKey(Map<String, Object> map, List<String> fields) {
        HashMap<String, Object> filedValue = new HashMap<>(fields.size() << 2);

        for (String key : fields) {
            filedValue.put(
                    key,
                    MapUtil.getValueByKey(
                            map, key, com.dtstack.chunjun.constants.ConstantValue.POINT_SYMBOL));
        }
        HashMap<String, Object> data = new HashMap<>();
        filedValue.forEach(
                (k, v) ->
                        MapUtil.buildMap(
                                k,
                                com.dtstack.chunjun.constants.ConstantValue.POINT_SYMBOL,
                                v,
                                data));
        return data;
    }

    /**
     * @param originalParamList 原始param配置
     * @param originBodyList 原始body配置
     * @param originalHeaderList 原始header配置
     * @param prevRequestParam 上一次请求的param参数
     * @param prevResponseValue 上一次请求返回的response值
     * @param restConfig 请求的配置信息
     * @param first 是否是第一次 影响后续取value还是nextValue
     * @param currentParam 当前请求参数 本次需要填充当前请求参数
     */
    public void buildParam(
            List<MetaParam> originalParamList,
            List<MetaParam> originBodyList,
            List<MetaParam> originalHeaderList,
            HttpRequestParam prevRequestParam,
            Map<String, Object> prevResponseValue,
            HttpRestConfig restConfig,
            boolean first,
            HttpRequestParam currentParam) {

        ArrayList<MetaParam> metaParams =
                new ArrayList<>(
                        originalParamList.size()
                                + originalHeaderList.size()
                                + originBodyList.size());
        metaParams.addAll(originalParamList);
        metaParams.addAll(originalHeaderList);
        metaParams.addAll(originBodyList);

        HttpRequestParam originParam = new HttpRequestParam();
        metaParams.forEach(i -> originParam.putValue(i, restConfig.getFieldDelimiter(), i));
        // 对header param header 参数解析，获取最终的值
        metaParams.forEach(
                i ->
                        getValue(
                                i,
                                originParam,
                                prevRequestParam,
                                prevResponseValue,
                                restConfig,
                                first,
                                currentParam));
    }

    /**
     * @param metaParam 本次解析的metaParam，获取最终的值
     * @param originParam 所有的原始请求参数 key和param对应关系 key如果是嵌套key已经拆分
     * @param prevRequestParam 上一次请求的参数
     * @param prevResponseValue 上一次返回的值
     * @param restConfig 请求的相关配置
     * @param first 是否是第一次 影响后续取value还是nextValue
     * @param currentParam 当前请求参数
     * @return 返回 metaParam 解析后的最终值
     */
    public Object getValue(
            MetaParam metaParam,
            HttpRequestParam originParam,
            HttpRequestParam prevRequestParam,
            Map<String, Object> prevResponseValue,
            HttpRestConfig restConfig,
            boolean first,
            HttpRequestParam currentParam) {

        // 根据是否是第一次 获取对应的值 value 还是nextValue
        String value = metaParam.getActualValue(first);

        String actualValue;

        // 如果已经加载过了 直接获取返回即可
        if (currentParam.containsKey(metaParam, restConfig.getFieldDelimiter())) {
            return currentParam.getValue(metaParam, restConfig.getFieldDelimiter());
        }

        List<Pair<MetaParam, String>> variableValues = new ArrayList<>();

        // 获取 value 里关联到的所有动态变量
        List<MetaParam> metaParams =
                MetaparamUtils.getValueOfMetaParams(
                        value, metaParam.getIsNest(), restConfig, originParam);

        // 对变量 按照内置变量还是其他变量分类  因为内置变量 currentTime intervalTime uuid 其实都是常量
        if (CollectionUtils.isNotEmpty(metaParams)) {
            metaParams.forEach(
                    i -> {
                        String variableValue;
                        if (i.getParamType().equals(ParamType.INNER)) {
                            variableValue = i.getValue();
                        } else if (i.getParamType().equals(ParamType.RESPONSE)) {
                            // 根据 返回值获取对应的值 response仍然是按照.进行切割的
                            variableValue =
                                    MapUtil.getValueByKey(
                                                    prevResponseValue,
                                                    i.getKey(),
                                                    com.dtstack.chunjun.constants.ConstantValue
                                                            .POINT_SYMBOL)
                                            .toString();
                        } else {
                            // 如果变量指向的是自己，那么就获取当前变量在上一次请求的值
                            if (i.getKey().equals(metaParam.getKey())) {
                                variableValue =
                                        prevRequestParam
                                                .getValue(
                                                        i,
                                                        metaParam.getIsNest()
                                                                ? restConfig.getFieldDelimiter()
                                                                : null)
                                                .toString();
                            } else {
                                // 如果是其他变量 就进行递归查找
                                variableValue =
                                        getValue(
                                                        i,
                                                        originParam,
                                                        prevRequestParam,
                                                        prevResponseValue,
                                                        restConfig,
                                                        first,
                                                        currentParam)
                                                .toString();
                            }
                        }
                        variableValues.add(Pair.of(i, variableValue));
                    });

            actualValue = calculateValue(metaParam, variableValues, first);

        } else {
            // 如果没有变量 就是常量
            actualValue = getData(value, metaParam);
        }

        currentParam.putValue(metaParam, restConfig.getFieldDelimiter(), actualValue);
        return actualValue;
    }

    /**
     * 将变量尽可能转为数字格式 如果字符串是日期格式，会尽量转为时间戳
     *
     * @param metaParam 需要解析的参数
     * @param variableValues 关联的其他动态参数和对应的值
     * @param first 是否是第一次
     */
    private BigDecimal getNumber(
            MetaParam metaParam, List<Pair<MetaParam, String>> variableValues, boolean first) {

        // 只支持一次加减 所以出现2个以上的动态变量是不符合要求的
        if (variableValues.size() > 2) {
            return null;
        }

        AtomicReference<String> value = new AtomicReference<>(metaParam.getActualValue(first));

        // -------  简单校验表达式是否符合a+-b的格式 -------------
        // a+-b 则表达式的长度至少是变量的长度加上运算符以及另一个计算变量的长度 所以表达式长度 > 变量长度+运算符长度
        if (variableValues.size() == 1
                && value.get().length()
                        > variableValues.get(0).getLeft().getVariableName().length() + 1) {
            HashSet<Character> characters = Sets.newHashSet('+', '-');
            String key = variableValues.get(0).getLeft().getVariableName();
            // 不是a+-b的都不符合要求
            if (value.get().startsWith(key)
                    && !characters.contains(value.get().charAt(key.length()))) {
                return null;
            } else if (value.get().endsWith(key)
                    && !characters.contains(
                            value.get().charAt(value.get().length() - key.length() - 1))) {
                return null;
            }
        } else if (variableValues.size() == 2) {
            List<String> strings =
                    variableValues.stream()
                            .map(i -> i.getLeft().getVariableName())
                            .collect(Collectors.toList());
            // 是不是a+-b的格式
            if (!Sets.newHashSet(
                            strings.get(0) + "+" + strings.get(1),
                            strings.get(0) + "-" + strings.get(1))
                    .contains(value.get())) {
                return null;
            }
        } else {
            return null;
        }

        try {
            BigDecimal decimal;
            for (Pair<MetaParam, String> pair : variableValues) {
                MetaParam left = pair.getLeft();
                String key = left.getVariableName();
                String right = pair.getRight();

                if (StringUtils.isEmpty(right)) {
                    return null;
                }

                // 如果是数字类型就直接替换
                if (NumberUtils.isCreatable(right)) {
                    value.set(
                            value.get()
                                    .replaceFirst(
                                            StringUtil.escapeExprSpecialWord(key),
                                            NumberUtils.createBigDecimal(right).toPlainString()));
                } else {

                    // 不是数字格式，就默认为是日期格式， 先是left解析  然后是 metaParam的去解析 然后是 默认的去解析，如果format解析失败
                    // 就直接返回null，当做字符串去拼接
                    if (Objects.nonNull(left.getTimeFormat())) {
                        try {
                            value.set(
                                    value.get()
                                            .replaceFirst(
                                                    StringUtil.escapeExprSpecialWord(key),
                                                    left.getTimeFormat().parse(right).getTime()
                                                            + ""));
                        } catch (ParseException e) {
                            log.info(
                                    left.getTimeFormat().toPattern()
                                            + " parse "
                                            + right
                                            + " failed,error info: "
                                            + ExceptionUtil.getErrorMessage(e));
                        }
                    }

                    try {
                        value.set(
                                value.get()
                                        .replaceFirst(
                                                StringUtil.escapeExprSpecialWord(key),
                                                DateUtil.stringToDate(
                                                                        right,
                                                                        metaParam.getTimeFormat())
                                                                .getTime()
                                                        + ""));
                    } catch (RuntimeException e1) {
                        log.info(
                                "parse "
                                        + right
                                        + " failed,error info "
                                        + ExceptionUtil.getErrorMessage(e1));
                        return null;
                    }
                }
            }

            String s = value.get();

            // 判断是否只有+-以及数字 且长度大于3（a+b最小是3）
            if (!p.matcher(s).find() || s.length() < 3) {
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
                // 从第一个字符开始找到第一个操作符的位置，防止第一位就是-，代表负数
                int i = StringUtils.indexOfIgnoreCase(s, "-", 1);
                a = s.substring(0, i);
                b = s.substring(i + 1);
            }
            if (NumberUtils.isCreatable(a) && NumberUtils.isCreatable(b)) {
                if ("+".equalsIgnoreCase(operation)) {
                    decimal = new BigDecimal(a).add(new BigDecimal(b));
                } else {
                    decimal = new BigDecimal(a).subtract(new BigDecimal(b));
                }
                return decimal;
            }

        } catch (Exception e) {
            log.warn(
                    "parse metaParam {} ,variableValues {},to BigDecimal, error info{}",
                    metaParam,
                    GsonUtil.GSON.toJson(variableValues),
                    ExceptionUtil.getErrorMessage(e));
            return null;
        }
        return null;
    }

    /**
     * 计算metaParam值
     *
     * @param metaParam 需要计算的metaParam
     * @param variableValues metaParam关联的变量以及变量对应的值
     * @param first 是否是第一次请求
     * @return metaParam对应的值
     */
    public String calculateValue(
            MetaParam metaParam, List<Pair<MetaParam, String>> variableValues, boolean first) {
        String value;
        if (CollectionUtils.isNotEmpty(variableValues)) {
            if (variableValues.size() == 1
                    && variableValues
                            .get(0)
                            .getLeft()
                            .getVariableName()
                            .equals(metaParam.getActualValue(first))) {
                value = variableValues.get(0).getRight();
            } else {
                BigDecimal o = getNumber(metaParam, variableValues, first);
                if (o == null) {
                    value = getString(metaParam, variableValues, first);
                } else {
                    value = o.stripTrailingZeros().toPlainString();
                }
            }
        } else {
            // 没有变量 就只是一个简单的字符串常量 这儿是进不来的 因为此方法是在 variableValues 不为空场景下才会调用的
            value = metaParam.getActualValue(first);
        }
        return getData(value, metaParam);
    }

    /**
     * 将metaParam转为的表达式 通过各个变量的字符串替换获取最终的值
     *
     * @param metaParam 需要解析的参数 metaParam
     * @param variableValues metaParam管理的各个变量以及其对应的值
     * @param first 是否是第一次请求
     * @return metaParam的值
     */
    private String getString(
            MetaParam metaParam, List<Pair<MetaParam, String>> variableValues, boolean first) {

        String value = metaParam.getActualValue(first);
        for (Pair<MetaParam, String> pair : variableValues) {
            value =
                    value.replaceFirst(
                            StringUtil.escapeExprSpecialWord(pair.getLeft().getVariableName()),
                            pair.getRight());
        }
        return value;
    }

    /**
     * 如果value是动态计算并且要求输出的格式是date格式 则format是必须要设置的， 否则计算出来的结果不是format格式的 而是时间戳格式
     *
     * @param value metaParam计算出来的尚未格式化的值
     * @param metaParam 变量metaParam
     * @return metaParam最终的值
     */
    public String getData(String value, MetaParam metaParam) {
        String data = value;
        if (Objects.nonNull(metaParam.getTimeFormat())) {
            try {
                long l = Long.parseLong(data);
                data = metaParam.getTimeFormat().format(new Date(l));
            } catch (Exception e) {
                // 如果date不是数字类型 就进入这里，认为是日期格式
                try {
                    data = metaParam.getTimeFormat().format(metaParam.getTimeFormat().parse(value));
                } catch (ParseException e1) {
                    throw new RuntimeException(
                            metaParam.getTimeFormat().toPattern()
                                    + "parse data["
                                    + value
                                    + "] error",
                            e);
                }
            }
        }
        return data;
    }
}
