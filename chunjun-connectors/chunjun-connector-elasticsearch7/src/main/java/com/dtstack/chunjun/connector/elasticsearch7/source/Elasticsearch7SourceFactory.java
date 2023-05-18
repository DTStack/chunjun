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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.elasticsearch.ElasticsearchRawTypeMapper;
import com.dtstack.chunjun.connector.elasticsearch.ElasticsearchSqlConverter;
import com.dtstack.chunjun.connector.elasticsearch.ElasticsearchSyncConverter;
import com.dtstack.chunjun.connector.elasticsearch7.ElasticsearchConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

public class Elasticsearch7SourceFactory extends SourceFactory {

    private final ElasticsearchConfig elasticsearchConfig;

    public Elasticsearch7SourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
        super(syncConfig, env);
        elasticsearchConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConfig.getReader().getParameter()),
                        ElasticsearchConfig.class);
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
        ElasticsearchInputFormatBuilder builder = new ElasticsearchInputFormatBuilder();
        builder.setEsConf(elasticsearchConfig);
        final RowType rowType =
                TableUtil.createRowType(elasticsearchConfig.getColumn(), getRawTypeMapper());
        AbstractRowConverter rowConverter;
        if (useAbstractBaseColumn) {
            rowConverter = new ElasticsearchSyncConverter(rowType);
        } else {
            rowConverter = new ElasticsearchSqlConverter(rowType);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        return createInput(builder.finish());
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return ElasticsearchRawTypeMapper::apply;
    }
}
