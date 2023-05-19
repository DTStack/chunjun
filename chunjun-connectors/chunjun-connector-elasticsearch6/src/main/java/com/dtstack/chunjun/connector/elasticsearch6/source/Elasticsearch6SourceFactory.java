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

package com.dtstack.chunjun.connector.elasticsearch6.source;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.elasticsearch.ElasticsearchRawTypeMapper;
import com.dtstack.chunjun.connector.elasticsearch.ElasticsearchSyncConverter;
import com.dtstack.chunjun.connector.elasticsearch6.Elasticsearch6Config;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

public class Elasticsearch6SourceFactory extends SourceFactory {

    private final Elasticsearch6Config elasticsearchConfig;

    public Elasticsearch6SourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env);
        elasticsearchConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConfig.getReader().getParameter()),
                        Elasticsearch6Config.class);
        List<FieldConfig> fieldList = syncConfig.getReader().getFieldList();
        String[] fieldNames = new String[fieldList.size()];
        for (int i = 0; i < fieldList.size(); i++) {
            fieldNames[i] = fieldList.get(i).getName();
        }

        super.initCommonConf(elasticsearchConfig);
        elasticsearchConfig.setColumn(fieldList);
        elasticsearchConfig.setFieldNames(fieldNames);
    }

    @Override
    public DataStream<RowData> createSource() {
        Elasticsearch6InputFormatBuilder builder = new Elasticsearch6InputFormatBuilder();
        builder.setEsConf(elasticsearchConfig);
        final RowType rowType =
                TableUtil.createRowType(elasticsearchConfig.getColumn(), getRawTypeMapper());
        builder.setRowConverter(new ElasticsearchSyncConverter(rowType));
        return createInput(builder.finish());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return ElasticsearchRawTypeMapper::apply;
    }
}
