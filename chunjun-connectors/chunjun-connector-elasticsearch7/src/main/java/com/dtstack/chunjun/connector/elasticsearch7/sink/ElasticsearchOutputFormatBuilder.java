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

package com.dtstack.chunjun.connector.elasticsearch7.sink;

import com.dtstack.chunjun.connector.elasticsearch.table.IndexGeneratorFactory;
import com.dtstack.chunjun.connector.elasticsearch7.ElasticsearchConfig;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.flink.table.catalog.ResolvedSchema;

import com.google.common.base.Preconditions;

public class ElasticsearchOutputFormatBuilder
        extends BaseRichOutputFormatBuilder<ElasticsearchOutputFormat> {

    public ElasticsearchOutputFormatBuilder(ElasticsearchConfig config, ResolvedSchema schema) {
        super(
                new ElasticsearchOutputFormat(
                        config,
                        IndexGeneratorFactory.createIndexGenerator(
                                config.getIndex(),
                                schema.getColumnNames(),
                                schema.getColumnDataTypes())));
        super.setConfig(config);
    }

    @Override
    protected void checkFormat() {
        ElasticsearchConfig esConfig = format.elasticsearchConfig;
        Preconditions.checkNotNull(
                esConfig.getHosts(), "elasticsearch7 type of address is required");
        Preconditions.checkNotNull(esConfig.getIndex(), "elasticsearch7 type of index is required");

        if (esConfig.getUsername() != null) {
            Preconditions.checkNotNull(
                    esConfig.getPassword(), "When set the username option, password is required");
        }
    }
}
