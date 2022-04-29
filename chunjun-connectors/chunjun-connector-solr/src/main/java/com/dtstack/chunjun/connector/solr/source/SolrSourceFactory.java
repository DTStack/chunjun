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

package com.dtstack.chunjun.connector.solr.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.solr.SolrConf;
import com.dtstack.chunjun.connector.solr.SolrConverterFactory;
import com.dtstack.chunjun.connector.solr.converter.SolrRawTypeConverter;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.source.SourceFactory;
import com.dtstack.chunjun.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/20
 */
public class SolrSourceFactory extends SourceFactory {

    private final SolrConf solrConf;

    public SolrSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        Gson gson = new GsonBuilder().create();
        GsonUtil.setTypeAdapter(gson);
        solrConf = gson.fromJson(gson.toJson(syncConf.getReader().getParameter()), SolrConf.class);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return SolrRawTypeConverter::apply;
    }

    @Override
    public DataStream<RowData> createSource() {
        SolrInputFormatBuilder builder = SolrInputFormatBuilder.newBuild(solrConf);
        SolrConverterFactory converterFactory = new SolrConverterFactory(solrConf);
        AbstractRowConverter converter;
        if (useAbstractBaseColumn) {
            converter = converterFactory.createColumnConverter();
        } else {
            converter = converterFactory.createRowConverter();
        }
        builder.setRowConverter(converter);
        return createInput(builder.finish());
    }
}
