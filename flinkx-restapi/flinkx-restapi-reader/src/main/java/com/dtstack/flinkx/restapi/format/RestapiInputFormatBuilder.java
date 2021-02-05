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
import com.dtstack.flinkx.restapi.client.MetaparamUtils;
import com.dtstack.flinkx.restapi.common.ConstantValue;
import com.dtstack.flinkx.restapi.common.HttpMethod;
import com.dtstack.flinkx.restapi.common.MetaParam;
import com.dtstack.flinkx.restapi.common.ParamType;
import com.dtstack.flinkx.restapi.reader.HttpRestConfig;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Function;
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
            errorMsg.append(String.format(errorTemplate, "format"));
        }
        if (format.httpRestConfig.getIntervalTime() == null) {
            errorMsg.append(String.format(errorTemplate, "intervalTime"));
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
                errorMsg.append("param 【strategy" + "】is not allow null when the job is not stream");
            } else if (format.httpRestConfig.getStrategy().stream().noneMatch(i -> i.getHandle().equals(ConstantValue.STRATEGY_STOP))) {
                errorMsg.append("param 【strategy" + "】must contains exit strategy  when the job is not stream");
            }
        }

        //循环依赖判断
        ArrayList<MetaParam> metaParams = new ArrayList<>(format.metaParams.size() + format.metaBodys.size() + format.metaHeaders.size());
        metaParams.addAll(format.metaParams);
        metaParams.addAll(format.metaBodys);
        metaParams.addAll(format.metaHeaders);

        Map<String, MetaParam> allParam = metaParams.stream().collect(Collectors.toMap(MetaParam::getAllName, Function.identity()));


        HashSet<String> anallyIng = new HashSet<>();
        HashSet<String> analyzed = new HashSet<>();

        metaParams.forEach(i -> {
            getValue(allParam, i, true, errorMsg, anallyIng, analyzed);
        });

        anallyIng.clear();
        analyzed.clear();

        metaParams.forEach(i -> {
            getValue(allParam, i, false, errorMsg, anallyIng, analyzed);
        });


        if (errorMsg.length() > 0) {
            throw new IllegalArgumentException(errorMsg.toString());
        }
    }

    public void getValue(Map<String, MetaParam> allParam, MetaParam metaParam, boolean first, StringBuilder errorMsg, HashSet<String> anallyIng, HashSet<String> analyzed) {
        anallyIng.add(metaParam.getAllName());
        ArrayList<MetaParam> collect = MetaparamUtils.getValueOfMetaParams(metaParam.getActualValue(first), format.httpRestConfig, allParam).stream().filter(i -> !analyzed.contains(i.getAllName()) || i.getParamType().equals(ParamType.BODY) || i.getParamType().equals(ParamType.PARAM) || i.getParamType().equals(ParamType.RESPONSE) || i.getParamType().equals(ParamType.HEADER)).collect(
                collectingAndThen(
                        toCollection(() -> new TreeSet<>(Comparator.comparing(MetaParam::getAllName))), ArrayList::new)
        );
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
                                getValue(allParam, i1, first, errorMsg, anallyIng, analyzed);
                            }
                        }
                    }
                }
        );
        anallyIng.remove(metaParam.getAllName());
        analyzed.add(metaParam.getAllName());
    }
}
