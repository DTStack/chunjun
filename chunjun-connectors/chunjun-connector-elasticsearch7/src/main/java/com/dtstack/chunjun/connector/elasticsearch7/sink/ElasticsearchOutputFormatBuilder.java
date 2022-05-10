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

import com.dtstack.chunjun.connector.elasticsearch.table.IndexGenerator;
import com.dtstack.chunjun.connector.elasticsearch.table.IndexGeneratorFactory;
import com.dtstack.chunjun.connector.elasticsearch7.ElasticsearchConf;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormatBuilder;

import org.apache.flink.table.api.TableSchema;

import com.google.common.base.Preconditions;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/27 17:19
 */
public class ElasticsearchOutputFormatBuilder extends BaseRichOutputFormatBuilder {

    protected ElasticsearchOutputFormat format;

    public ElasticsearchOutputFormatBuilder(ElasticsearchConf conf, TableSchema schema) {
        IndexGenerator indexGenerator =
                IndexGeneratorFactory.createIndexGenerator(conf.getIndex(), schema);
        super.format = format = new ElasticsearchOutputFormat(conf, indexGenerator);
        super.setConfig(conf);
    }

    @Override
    protected void checkFormat() {
        ElasticsearchConf esConf = format.elasticsearchConf;
        Preconditions.checkNotNull(esConf.getHosts(), "elasticsearch7 type of address is required");
        Preconditions.checkNotNull(esConf.getIndex(), "elasticsearch7 type of index is required");

        if (esConf.getUsername() != null) {
            Preconditions.checkNotNull(
                    esConf.getPassword(), "When set the username option, password is required");
        }
    }
}
