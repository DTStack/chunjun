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

package com.dtstack.chunjun.connector.elasticsearch5.sink;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.elasticsearch5.conf.ElasticsearchConf;
import com.dtstack.chunjun.connector.elasticsearch5.converter.ElasticsearchColumnConverter;
import com.dtstack.chunjun.connector.elasticsearch5.converter.ElasticsearchRawTypeConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * @description:
 * @program chunjun
 * @author: lany
 * @create: 2021/06/27 23:49
 */
public class Elasticsearch5SinkFactory extends SinkFactory {

    private final ElasticsearchConf elasticsearchConf;

    public Elasticsearch5SinkFactory(SyncConf syncConf) {
        super(syncConf);
        elasticsearchConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()),
                        ElasticsearchConf.class);
        elasticsearchConf.setColumn(syncConf.getWriter().getFieldList());
        super.initCommonConf(elasticsearchConf);
        elasticsearchConf.setParallelism(1);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        ElasticsearchOutputFormatBuilder builder = new ElasticsearchOutputFormatBuilder();
        builder.setEsConf(elasticsearchConf);
        final RowType rowType =
                TableUtil.createRowType(elasticsearchConf.getColumn(), getRawTypeConverter());
        builder.setRowConverter(new ElasticsearchColumnConverter(rowType));
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return ElasticsearchRawTypeConverter::apply;
    }
}
