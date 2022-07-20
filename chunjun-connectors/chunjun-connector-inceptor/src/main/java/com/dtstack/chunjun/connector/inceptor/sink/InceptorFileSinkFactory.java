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

package com.dtstack.chunjun.connector.inceptor.sink;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.inceptor.conf.InceptorFileConf;
import com.dtstack.chunjun.connector.inceptor.converter.InceptorHdfsRawTypeConverter;
import com.dtstack.chunjun.connector.inceptor.enums.ECompressType;
import com.dtstack.chunjun.connector.inceptor.util.InceptorUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import java.util.Map;

public class InceptorFileSinkFactory extends SinkFactory {
    private InceptorFileConf inceptorFileConf;

    public InceptorFileSinkFactory(SyncConf syncConf) {
        super(syncConf);
        this.inceptorFileConf =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(syncConf.getWriter().getParameter()),
                        InceptorFileConf.class);
        inceptorFileConf.setColumn(syncConf.getWriter().getFieldList());
        super.initCommonConf(inceptorFileConf);
        Map<String, Object> parameter = syncConf.getWriter().getParameter();
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
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        InceptorFileOutputFormatBuilder builder =
                InceptorFileOutputFormatBuilder.newBuilder(inceptorFileConf.getFileType());
        builder.setInceptorConf(inceptorFileConf);
        builder.setCompressType(
                ECompressType.getByTypeAndFileType(
                        inceptorFileConf.getCompress(), inceptorFileConf.getFileType()));
        AbstractRowConverter rowConverter =
                InceptorUtil.createRowConverter(
                        useAbstractBaseColumn,
                        inceptorFileConf.getFileType(),
                        inceptorFileConf.getColumn(),
                        getRawTypeConverter());

        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return InceptorHdfsRawTypeConverter::apply;
    }
}
