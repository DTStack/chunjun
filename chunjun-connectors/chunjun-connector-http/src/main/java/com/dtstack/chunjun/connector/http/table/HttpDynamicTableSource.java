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

package com.dtstack.chunjun.connector.http.table;

import com.dtstack.chunjun.connector.http.common.ConstantValue;
import com.dtstack.chunjun.connector.http.common.HttpMethod;
import com.dtstack.chunjun.connector.http.common.HttpRestConfig;
import com.dtstack.chunjun.connector.http.common.MetaParam;
import com.dtstack.chunjun.connector.http.common.ParamType;
import com.dtstack.chunjun.connector.http.converter.HttpSqlConverter;
import com.dtstack.chunjun.connector.http.inputformat.HttpInputFormatBuilder;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @author shifang
 * @create 2021-04-09 09:20
 * @description
 */
public class HttpDynamicTableSource implements ScanTableSource {
    private final TableSchema schema;
    private final HttpRestConfig httpRestConfig;

    public HttpDynamicTableSource(TableSchema schema, HttpRestConfig httpRestConfig) {
        this.schema = schema;
        this.httpRestConfig = httpRestConfig;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        HttpInputFormatBuilder builder = new HttpInputFormatBuilder();
        builder.setHttpRestConfig(httpRestConfig);
        builder.setRowConverter(
                new HttpSqlConverter(
                        (RowType) this.schema.toRowDataType().getLogicalType(), httpRestConfig));
        builder.setMetaHeaders(httpRestConfig.getHeader());
        builder.setMetaParams(
                httpRestConfig.getParam() == null ? new ArrayList<>() : httpRestConfig.getParam());
        builder.setMetaBodies(httpRestConfig.getBody());
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
        MetaParam.setMetaColumnsType(httpRestConfig.getBody(), ParamType.BODY);
        MetaParam.setMetaColumnsType(httpRestConfig.getParam(), ParamType.PARAM);
        MetaParam.setMetaColumnsType(httpRestConfig.getHeader(), ParamType.HEADER);

        MetaParam.initTimeFormat(httpRestConfig.getBody());
        MetaParam.initTimeFormat(httpRestConfig.getParam());
        MetaParam.initTimeFormat(httpRestConfig.getHeader());
        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation), false, 1);
    }

    @Override
    public DynamicTableSource copy() {
        return new HttpDynamicTableSource(this.schema, this.httpRestConfig);
    }

    @Override
    public String asSummaryString() {
        return "RestapiDynamicTableSource:";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}
