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

package com.dtstack.chunjun.connector.elasticsearch7.source;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.elasticsearch.ElasticsearchColumnConverter;
import com.dtstack.chunjun.connector.elasticsearch.ElasticsearchRawTypeMapper;
import com.dtstack.chunjun.connector.elasticsearch.ElasticsearchRowConverter;
import com.dtstack.chunjun.connector.elasticsearch7.ElasticsearchConf;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * @description:
 * @program chunjun
 * @author: lany
 * @create: 2021/06/27 17:28
 */
public class Elasticsearch7SourceFactory extends SourceFactory {

    private final ElasticsearchConf elasticsearchConf;

    public Elasticsearch7SourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        elasticsearchConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getReader().getParameter()),
                        ElasticsearchConf.class);
        List<FieldConf> fieldList = syncConf.getReader().getFieldList();
        String[] fieldNames = new String[fieldList.size()];
        for (int i = 0; i < fieldList.size(); i++) {
            fieldNames[i] = fieldList.get(i).getName();
        }

        super.initCommonConf(elasticsearchConf);
        elasticsearchConf.setColumn(fieldList);
        elasticsearchConf.setFieldNames(fieldNames);
    }

    @Override
    public DataStream<RowData> createSource() {
        ElasticsearchInputFormatBuilder builder = new ElasticsearchInputFormatBuilder();
        builder.setEsConf(elasticsearchConf);
        final RowType rowType =
                TableUtil.createRowType(elasticsearchConf.getColumn(), getRawTypeConverter());
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter = new ElasticsearchColumnConverter(rowType);
        } else {
            rowConverter = new ElasticsearchRowConverter(rowType);
        }
        builder.setRowConverter(rowConverter);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return ElasticsearchRawTypeMapper::apply;
    }
}
