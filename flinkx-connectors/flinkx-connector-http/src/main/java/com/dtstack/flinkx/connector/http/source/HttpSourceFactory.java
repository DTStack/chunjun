/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.connector.http.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.http.common.ConstantValue;
import com.dtstack.flinkx.connector.http.common.HttpMethod;
import com.dtstack.flinkx.connector.http.common.HttpRestConfig;
import com.dtstack.flinkx.connector.http.common.MetaParam;
import com.dtstack.flinkx.connector.http.common.ParamType;
import com.dtstack.flinkx.connector.http.converter.HttpColumnConverter;
import com.dtstack.flinkx.connector.http.converter.HttpRawTypeConverter;
import com.dtstack.flinkx.connector.http.converter.HttpRowConverter;
import com.dtstack.flinkx.connector.http.inputformat.HttpInputFormatBuilder;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.util.JsonUtil;
import com.dtstack.flinkx.util.StringUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @author: shifang
 * @create: 2019/7/4
 */
public class HttpSourceFactory extends SourceFactory {

    private final HttpRestConfig httpRestConfig;

    public HttpSourceFactory(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        httpRestConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(config.getReader().getParameter()), HttpRestConfig.class);
        MetaParam.setMetaColumnsType(httpRestConfig.getBody(), ParamType.BODY);
        MetaParam.setMetaColumnsType(httpRestConfig.getParam(), ParamType.PARAM);
        MetaParam.setMetaColumnsType(httpRestConfig.getHeader(), ParamType.HEADER);
        MetaParam.initTimeFormat(httpRestConfig.getBody());
        MetaParam.initTimeFormat(httpRestConfig.getParam());
        MetaParam.initTimeFormat(httpRestConfig.getHeader());
        // post请求 如果contentTy没有设置，则默认设置为 application/json
        if (HttpMethod.POST.name().equalsIgnoreCase(httpRestConfig.getRequestMode())
                && httpRestConfig.getHeader().stream()
                        .noneMatch(i -> ConstantValue.CONTENT_TYPE_NAME.equals(i.getKey()))) {
            if (CollectionUtils.isEmpty(httpRestConfig.getHeader())) {
                httpRestConfig.setHeader(
                        Collections.singletonList(
                                new MetaParam(
                                        ConstantValue.CONTENT_TYPE_NAME,
                                        ConstantValue.CONTENT_TYPE_DEFAULT_VALUE,
                                        ParamType.HEADER)));
            } else {
                httpRestConfig
                        .getHeader()
                        .add(
                                new MetaParam(
                                        ConstantValue.CONTENT_TYPE_NAME,
                                        ConstantValue.CONTENT_TYPE_DEFAULT_VALUE,
                                        ParamType.HEADER));
            }
        }
        if (syncConf.getTransformer() == null
                || org.apache.commons.lang3.StringUtils.isBlank(
                        syncConf.getTransformer().getTransformSql())) {
            typeInformation =
                    TableUtil.getTypeInformation(Collections.emptyList(), getRawTypeConverter());
        } else {
            typeInformation =
                    TableUtil.getTypeInformation(
                            subColumns(httpRestConfig.getColumn()), getRawTypeConverter());
            useAbstractBaseColumn = false;
        }
        super.initFlinkxCommonConf(httpRestConfig);
    }

    private List<FieldConf> subColumns(List<FieldConf> fields) {
        List<FieldConf> columnsNoDelimiter = new ArrayList();
        fields.forEach(
                fieldConf -> {
                    FieldConf newField = new FieldConf();
                    String[] split =
                            fieldConf
                                    .getName()
                                    .split(
                                            StringUtil.escapeExprSpecialWord(
                                                    httpRestConfig.getFieldDelimiter()));
                    newField.setName(split[split.length - 1]);
                    newField.setType(fieldConf.getType());
                    columnsNoDelimiter.add(newField);
                });
        return columnsNoDelimiter;
    }

    @Override
    public DataStream<RowData> createSource() {
        HttpInputFormatBuilder builder = new HttpInputFormatBuilder();
        AbstractRowConverter rowConverter = null;
        if (useAbstractBaseColumn) {
            rowConverter = new HttpColumnConverter(httpRestConfig);
        } else {
            final RowType rowType =
                    TableUtil.createRowType(httpRestConfig.getColumn(), getRawTypeConverter());
            rowConverter = new HttpRowConverter(rowType, httpRestConfig);
        }
        builder.setHttpRestConfig(httpRestConfig);
        builder.setMetaHeaders(httpRestConfig.getHeader());
        builder.setMetaParams(httpRestConfig.getParam());
        builder.setMetaBodys(httpRestConfig.getBody());
        builder.setRowConverter(rowConverter);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return HttpRawTypeConverter::apply;
    }
}
