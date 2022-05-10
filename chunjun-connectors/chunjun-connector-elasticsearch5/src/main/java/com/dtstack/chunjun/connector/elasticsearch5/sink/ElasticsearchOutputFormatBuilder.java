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

import com.dtstack.chunjun.connector.elasticsearch5.conf.ElasticsearchConf;
import com.dtstack.chunjun.outputformat.BaseRichOutputFormatBuilder;

import com.google.common.base.Preconditions;

/**
 * @description:
 * @program: chunjun-all
 * @author: lany
 * @create: 2021/06/27 23:51
 */
public class ElasticsearchOutputFormatBuilder extends BaseRichOutputFormatBuilder {
    protected ElasticsearchOutputFormat format;

    public ElasticsearchOutputFormatBuilder() {
        super.format = format = new ElasticsearchOutputFormat();
    }

    public void setEsConf(ElasticsearchConf esConf) {
        super.setConfig(esConf);
        format.setElasticsearchConf(esConf);
    }

    @Override
    protected void checkFormat() {
        ElasticsearchConf esConf = format.getElasticsearchConf();
        Preconditions.checkNotNull(esConf.getHosts(), "elasticsearch5 type of address is required");
        Preconditions.checkNotNull(esConf.getIndex(), "elasticsearch5 type of index is required");
        Preconditions.checkNotNull(esConf.getType(), "elasticsearch5 type of type is required");
        Preconditions.checkNotNull(
                esConf.getCluster(), "elasticsearch5 type of cluster is required");

        /** is open basic auth */
        if (esConf.isAuthMesh()) {
            Preconditions.checkNotNull(
                    esConf.getUsername(), "elasticsearch5 type of userName is required");
            Preconditions.checkNotNull(
                    esConf.getPassword(), "elasticsearch5 type of password is required");
        }
    }
}
