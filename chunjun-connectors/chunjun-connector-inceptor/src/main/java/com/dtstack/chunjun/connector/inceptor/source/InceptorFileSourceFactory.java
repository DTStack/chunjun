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

package com.dtstack.chunjun.connector.inceptor.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.inceptor.conf.InceptorFileConf;
import com.dtstack.chunjun.connector.inceptor.converter.InceptorHdfsRawTypeConverter;
import com.dtstack.chunjun.connector.inceptor.util.InceptorUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.util.Map;

public class InceptorFileSourceFactory extends SourceFactory {

    private InceptorFileConf inceptorFileConf;

    protected InceptorFileSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        this.inceptorFileConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(syncConf.getReader().getParameter()),
                        InceptorFileConf.class);
        inceptorFileConf.setColumn(syncConf.getReader().getFieldList());
        super.initCommonConf(inceptorFileConf);

        Map<String, Object> parameter = syncConf.getReader().getParameter();
        if (null != parameter.get("defaultFS")) {
            inceptorFileConf.setDefaultFs(parameter.get("defaultFS").toString());
        }

        if (null != parameter.get("isTransaction")) {
            inceptorFileConf.setTransaction((Boolean) parameter.get("isTransaction"));
        }

        if (parameter.get("fieldDelimiter") == null
                || parameter.get("fieldDelimiter").toString().length() == 0) {
            inceptorFileConf.setFieldDelimiter("\001");
        } else {
            inceptorFileConf.setFieldDelimiter(
                    com.dtstack.chunjun.util.StringUtil.convertRegularExpr(
                            parameter.get("fieldDelimiter").toString()));
        }
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return InceptorHdfsRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        InceptorFileInputFormatBuilder builder =
                InceptorFileInputFormatBuilder.newBuild(inceptorFileConf.getFileType());

        builder.setHdfsConf(inceptorFileConf);
        AbstractRowConverter rowConverter =
                InceptorUtil.createRowConverter(
                        useAbstractBaseColumn,
                        inceptorFileConf.getFileType(),
                        inceptorFileConf.getColumn(),
                        getRawTypeConverter());

        builder.setRowConverter(rowConverter);
        return createInput(builder.finish());
    }
}
