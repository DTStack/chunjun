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
package com.dtstack.flinkx.restapi.format;

import com.dtstack.flinkx.inputformat.BaseRichInputFormatBuilder;
import com.dtstack.flinkx.restapi.client.HttpRequestParam;
import com.dtstack.flinkx.restapi.client.MetaparamUtils;
import com.dtstack.flinkx.restapi.client.Strategy;
import com.dtstack.flinkx.restapi.common.ConstantValue;
import com.dtstack.flinkx.restapi.common.HttpMethod;
import com.dtstack.flinkx.restapi.common.MetaParam;
import com.dtstack.flinkx.restapi.common.ParamType;
import com.dtstack.flinkx.restapi.reader.HttpRestConfig;
import com.dtstack.flinkx.util.GsonUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;

/**
 * @author : tiezhu
 * @date : 2020/3/12
 */
public class RestapiInputFormatBuilder extends BaseRichInputFormatBuilder {
    protected RestapiInputFormat format;


    public RestapiInputFormatBuilder() {
        super.format = format = new RestapiInputFormat();
    }

    public void setHttpRestConfig(HttpRestConfig httpRestConfig) {
        this.format.setHttpRestConfig(httpRestConfig);
    }

    public void setMetaParams(List<MetaParam> metaColumns) {
        this.format.metaParams = metaColumns;
    }

    public void setMetaBodys(List<MetaParam> metaColumns) {
        this.format.metaBodys = metaColumns;
    }

    public void setMetaHeaders(List<MetaParam> metaColumns) {
        this.format.metaHeaders = metaColumns;
    }

    public void setStream(boolean stream) {
        this.format.isStream = stream;
    }

    @Override
    protected void checkFormat() {

        StringBuilder errorMsg = new StringBuilder(128);
        String errorTemplate = "param 【%s】 is not allow null \n";


        if (StringUtils.isBlank(format.httpRestConfig.getUrl())) {
            errorMsg.append(String.format(errorTemplate, "url"));
        }
        if (StringUtils.isBlank(format.httpRestConfig.getRequestMode())) {
            errorMsg.append(String.format(errorTemplate, "requestMode"));
        } else {

            if (!Sets.newHashSet(HttpMethod.GET.name(), HttpMethod.POST.name()).contains(format.httpRestConfig.getRequestMode().toUpperCase(Locale.ENGLISH))) {
                errorMsg.append("requestMode just support GET and POST,we not support ").append(format.httpRestConfig.getRequestMode()).append(" \n");
            }
        }
        if (StringUtils.isBlank(format.httpRestConfig.getDecode())) {
            errorMsg.append(String.format(errorTemplate, "format")).append("\n");
        }
        if (format.httpRestConfig.getIntervalTime() == null) {
            errorMsg.append(String.format(errorTemplate, "intervalTime")).append("\n");
        } else if (format.httpRestConfig.getIntervalTime() <= 0) {
            errorMsg.append("param 【intervalTime" + "】must more than 0 \n");
        }

        //如果是post请求 但是contentType不是application/json就直接报错
        if (HttpMethod.POST.name().equalsIgnoreCase(format.httpRestConfig.getRequestMode())) {
            format.metaHeaders.stream()
                    .filter(i -> ConstantValue.CONTENT_TYPE_NAME.equals(i.getKey()) && !ConstantValue.CONTENT_TYPE_DEFAULT_VALUE.equals(i.getValue()))
                    .findFirst().ifPresent(i -> {
                errorMsg.append("header 【").append(i.getKey()).append("】not support ").append(i.getValue()).append(" we just support application/json when requestMode is post \n");
            });


        }
        //如果是离线任务 必须有策略是停止策略
        if (!this.format.isStream) {
            if (CollectionUtils.isEmpty(format.httpRestConfig.getStrategy())) {
                errorMsg.append("param 【strategy" + "】is not allow null when the job is not stream").append("\n");
            } else if (format.httpRestConfig.getStrategy().stream().noneMatch(i -> i.getHandle().equals(ConstantValue.STRATEGY_STOP))) {
                errorMsg.append("param 【strategy" + "】must contains exit strategy  when the job is not stream").append("\n");
            }
        }


        if (StringUtils.isEmpty(format.httpRestConfig.getFieldDelimiter()) || !ConstantValue.FIELD_DELIMITER.contains(format.httpRestConfig.getFieldDelimiter())) {
            errorMsg.append("we just support fieldDelimiter ").append(GsonUtil.GSON.toJson(ConstantValue.FIELD_DELIMITER)).append("\n");
        }

        if(errorMsg.length() > 0){
            throw new IllegalArgumentException(errorMsg.toString());
        }

        //则需要判断是否存在key相同的场景 key相同但是一个是嵌套key且层级必须大于1级一个是非嵌套key是可以的
        ArrayList<MetaParam> metaParams = new ArrayList<>(format.metaParams.size() + format.metaBodys.size() + format.metaHeaders.size());
        metaParams.addAll(format.metaParams);
        metaParams.addAll(format.metaBodys);
        metaParams.addAll(format.metaHeaders);


        StringBuilder sb = new StringBuilder();

        Set<String> allowedRepeatedName = new HashSet<>(format.metaParams.size() + format.metaHeaders.size() + format.metaBodys.size());
        allowedRepeatedName.addAll(getAllowedRepeatedName(format.metaParams, sb));
        allowedRepeatedName.addAll(getAllowedRepeatedName(format.metaBodys, sb));
        allowedRepeatedName.addAll(getAllowedRepeatedName(format.metaHeaders, sb));

        //将原始参数 按照key 放入各自所属map中 如果是嵌套key 会切割后放入对应层次中
        HttpRequestParam originParam = new HttpRequestParam();
        metaParams.forEach(i -> {
            originParam.putValue(i, format.httpRestConfig.getFieldDelimiter(), i);
        });

        //如果存在相同key 且一个是嵌套key 一个是非嵌套key  那么此时需要判断 异常策略里是否有指定对应的key  因为异常策略里指定了key 但不知道是嵌套key 还是非嵌套key
        if (CollectionUtils.isNotEmpty(allowedRepeatedName) && CollectionUtils.isNotEmpty(format.httpRestConfig.getStrategy())) {
            List<Strategy> errorStrategy = new ArrayList<>();

            format.httpRestConfig.getStrategy().forEach(i -> {
                MetaparamUtils.getValueOfMetaParams(i.getKey(),null, format.httpRestConfig, originParam).forEach(p -> {
                    if (allowedRepeatedName.contains(p.getAllName())) {
                        errorStrategy.add(i);
                       }
                });

                MetaparamUtils.getValueOfMetaParams(i.getValue(),null, format.httpRestConfig, originParam).forEach(p -> {
                    if (allowedRepeatedName.contains(p.getAllName())) {
                        errorStrategy.add(i);
                      }
                });
            });

            if(CollectionUtils.isNotEmpty(errorStrategy)){
                sb.append(StringUtils.join(errorStrategy,",")).append(" because we do not know specified key or value is nested or non nested").append("\n");
            }

        }

        if (sb.length() > 0) {
            throw new IllegalArgumentException(errorMsg.append(sb).toString());
        }


        //循环依赖判断
        Set<String> anallyIng = new HashSet<>();
        Set<String> analyzed = new HashSet<>();
        metaParams.forEach(i -> {
            getValue(originParam, i, true, errorMsg, anallyIng, analyzed, allowedRepeatedName);
        });

        anallyIng.clear();
        analyzed.clear();

        metaParams.forEach(i -> {
            getValue(originParam, i, false, errorMsg, anallyIng, analyzed, allowedRepeatedName);
        });


        if (errorMsg.length() > 0) {
            throw new IllegalArgumentException(errorMsg.toString());
        }
    }


    public void getValue(HttpRequestParam requestParam, MetaParam metaParam, boolean first, StringBuilder errorMsg, Set<String> anallyIng, Set<String> analyzed, Set<String> sameNames) {

        anallyIng.add(metaParam.getAllName());
        ArrayList<MetaParam> collect = MetaparamUtils.getValueOfMetaParams(metaParam.getActualValue(first),metaParam.getNest(), format.httpRestConfig, requestParam).stream().filter(i -> !analyzed.contains(i.getAllName()) || i.getParamType().equals(ParamType.BODY) || i.getParamType().equals(ParamType.PARAM) || i.getParamType().equals(ParamType.RESPONSE) || i.getParamType().equals(ParamType.HEADER)).collect(
                collectingAndThen(
                        toCollection(() -> new TreeSet<>(Comparator.comparing(MetaParam::getAllName))), ArrayList::new)
        );

        //如果存在相同key 判断是否有value|nextValue指定了该key 因为不知道是指向嵌套key还是指向非嵌套key
        if(CollectionUtils.isNotEmpty(sameNames)){
            Set<String> keys = collect.stream().filter(i -> i.getParamType().equals(ParamType.HEADER) || i.getParamType().equals(ParamType.BODY) || i.getParamType().equals(ParamType.PARAM)).map(MetaParam::getAllName).collect(Collectors.toSet());
            StrBuilder sb = new StrBuilder(128);
            for (String key : keys) {
                if (sameNames.contains(key)) {
                    sb.append(key).append(" ");
                }
            }

            if (sb.length() > 0) {
                throw new IllegalArgumentException(metaParam.getActualValue(first) + " on " + requestParam + "  is error because we do not know " + sb.toString() + "is nest or not");
            }
        }


        collect.forEach(i1 -> {
                    //value变量里不能有response变量  因为value变量是第一次请求的key，此时还没有response
                    if (first && i1.getParamType().equals(ParamType.RESPONSE)) {
                        errorMsg.append(i1.getAllName()).append(" can not has response variable in value \n");
                        //value变量里不能指向自己，因为此时value还没有初始值，只有nextValue里的变量可以指向自己
                    } else if (first && i1.getAllName().equals(metaParam.getAllName())) {
                        errorMsg.append(" The variable in the value of ").append(i1.getAllName()).append(" can not point to itself \n");
                    } else if (i1.getParamType().equals(ParamType.PARAM) || i1.getParamType().equals(ParamType.BODY) || i1.getParamType().equals(ParamType.HEADER)) {

                        //如果这个变量是指向自己的 那么就直接跳过 不需要解析
                        if (!i1.getAllName().equals(metaParam.getAllName())) {
                            if (anallyIng.contains(i1.getAllName())) {
                                errorMsg.append(metaParam.getAllName()).append(" and ").append(i1.getAllName()).append(" are cyclically dependent \n");
                                //发生循环依赖就直接报错
                                throw new IllegalArgumentException(errorMsg.toString());
                            } else if (!analyzed.contains(i1.getAllName())) {
                                getValue(requestParam, i1, first, errorMsg, anallyIng, analyzed, sameNames);
                            }
                        }
                    }
                }
        );
        anallyIng.remove(metaParam.getAllName());
        analyzed.add(metaParam.getAllName());
    }

    /**
     * 找出 metaParams 里相同key的metaParam
     * @param metaParams
     * @return
     */
    public List<MetaParam> repeatParamByKey(List<MetaParam> metaParams) {
        ArrayList<MetaParam> data = new ArrayList<>(metaParams.size());
        Map<String, List<MetaParam>> map = metaParams.stream().collect(Collectors.groupingBy(MetaParam::getAllName));
        map.forEach((k, v) -> {
            if (v.size() > 1) {
                data.addAll(v);
            }
        });
        return data;
    }

    /**
     * 找出两个list之间MetaParam的key是相同的
     * @param left
     * @param right
     * @return
     */
    public List<MetaParam> repeatParamByKey(List<MetaParam> left, List<MetaParam> right) {
        ArrayList<MetaParam> data = new ArrayList<>(left.size());
        Set<String> keys = right.stream().map(MetaParam::getKey).collect(Collectors.toSet());
        left.forEach(i -> {
            if (keys.contains(i.getKey())) {
                data.add(i);
            }
        });

        return data;
    }

    /**
     * 获取允许相同key的参数  如果一个是嵌套key且层级大于1级 一个是非嵌套key
     * @param params 请求参数
     * @param sb 错误信息
     * @return 允许相同的key 前缀加上各自类型 如body.key1 header.key1
     */
    private Set<String> getAllowedRepeatedName(List<MetaParam> params, StringBuilder sb) {
        //按照是否是嵌套key进行划分
        Map<Boolean, List<MetaParam>> paramMap = params.stream().collect(Collectors.groupingBy(MetaParam::getNest));
        List<MetaParam> repeatParam = new ArrayList<>(32);

        if (CollectionUtils.isNotEmpty(paramMap.get(true))) {
            repeatParam.addAll(repeatParamByKey(paramMap.get(true)));
        }

        if (CollectionUtils.isNotEmpty(paramMap.get(false))) {
            repeatParam.addAll(repeatParamByKey(paramMap.get(false)));
        }


        if (CollectionUtils.isNotEmpty(repeatParam)) {
            sb.append("key can not repeat,key is ").append(StringUtils.join(repeatParam.stream().map(MetaParam::getAllName).collect(Collectors.toSet()), ",")).append("\n");
        }


        HashSet<String> allowedSameNames = new HashSet<>();
        // 嵌套和非嵌套是否有重复的
        if (paramMap.keySet().size() == 2) {
            if (paramMap.get(true).size() >= paramMap.get(false).size()) {
                repeatParam = repeatParamByKey(paramMap.get(true), paramMap.get(false));
            } else {
                repeatParam = repeatParamByKey(paramMap.get(false), paramMap.get(true));
            }

            repeatParam.forEach(i -> {
                if (i.getKey().split(StringUtil.escapeExprSpecialWord(format.httpRestConfig.getFieldDelimiter())).length == 1) {
                    sb.append(i.toString()).append(" is repeated and it has only one level When it is a nested key").append("\n");
                }else{
                    allowedSameNames.add(i.getAllName());
                }
            });
        }
        return allowedSameNames;
    }
}
