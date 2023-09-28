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

package com.dtstack.chunjun.connector.http.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.http.common.ConstantValue;
import com.dtstack.chunjun.connector.http.common.HttpMethod;
import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.connector.http.common.MetaParam;
import com.dtstack.chunjun.connector.http.common.ParamType;
import com.dtstack.chunjun.connector.http.converter.HttpRawTypeMapper;
import com.dtstack.chunjun.connector.http.converter.HttpSqlConverter;
import com.dtstack.chunjun.connector.http.converter.HttpSyncConverter;
import com.dtstack.chunjun.connector.http.inputformat.HttpInputFormatBuilder;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.StringUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HttpSourceFactory extends SourceFactory {

    private final HttpRestConfig httpRestConfig;

    public HttpSourceFactory(SyncConfig config, StreamExecutionEnvironment env) {
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
        if (syncConfig.getTransformer() == null
                || StringUtils.isBlank(syncConfig.getTransformer().getTransformSql())) {
            typeInformation =
                    TableUtil.getTypeInformation(Collections.emptyList(), getRawTypeMapper(), true);
        } else {
            typeInformation =
                    TableUtil.getTypeInformation(
                            subColumns(httpRestConfig.getColumn()), getRawTypeMapper(), false);
            useAbstractBaseColumn = false;
        }
        super.initCommonConf(httpRestConfig);
    }

    private List<FieldConfig> subColumns(List<FieldConfig> fields) {
        List<FieldConfig> columnsNoDelimiter = new ArrayList();
        fields.forEach(
                fieldConfig -> {
                    FieldConfig newField = new FieldConfig();
                    String[] split =
                            fieldConfig
                                    .getName()
                                    .split(
                                            StringUtil.escapeExprSpecialWord(
                                                    httpRestConfig.getFieldDelimiter()));
                    newField.setName(split[split.length - 1]);
                    newField.setType(fieldConfig.getType());
                    columnsNoDelimiter.add(newField);
                });
        return columnsNoDelimiter;
    }

    @Override
    public DataStream<RowData> createSource() {
        HttpInputFormatBuilder builder = new HttpInputFormatBuilder();
        AbstractRowConverter rowConverter = null;
        if (useAbstractBaseColumn) {
            rowConverter = new HttpSyncConverter(httpRestConfig);
        } else {
            final RowType rowType =
                    TableUtil.createRowType(httpRestConfig.getColumn(), getRawTypeMapper());
            rowConverter = new HttpSqlConverter(rowType, httpRestConfig);
        }
        builder.setHttpRestConfig(httpRestConfig);
        builder.setMetaHeaders(httpRestConfig.getHeader());
        builder.setMetaParams(httpRestConfig.getParam());
        builder.setMetaBodies(httpRestConfig.getBody());
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return HttpRawTypeMapper::apply;
    }
}
