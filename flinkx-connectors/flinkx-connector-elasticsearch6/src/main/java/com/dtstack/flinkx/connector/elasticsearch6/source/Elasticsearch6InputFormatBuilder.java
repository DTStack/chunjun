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

package com.dtstack.flinkx.connector.elasticsearch6.source;

import com.dtstack.flinkx.connector.elasticsearch6.Elasticsearch6Conf;
import com.dtstack.flinkx.source.format.BaseRichInputFormatBuilder;

import com.google.common.base.Preconditions;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/06/18 12:00
 */
public class Elasticsearch6InputFormatBuilder extends BaseRichInputFormatBuilder {

    protected Elasticsearch6InputFormat format;

    public Elasticsearch6InputFormatBuilder() {
        super.format = this.format = new Elasticsearch6InputFormat();
    }

    public void setEsConf(Elasticsearch6Conf esConf) {
        super.setConfig(esConf);
        format.setElasticsearchConf(esConf);
    }

    @Override
    protected void checkFormat() {
        Elasticsearch6Conf esConf = format.getElasticsearchConf();
        Preconditions.checkNotNull(esConf.getHosts(), "elasticsearch6 type of address is required");
        Preconditions.checkNotNull(esConf.getIndex(), "elasticsearch6 type of index is required");
        Preconditions.checkNotNull(esConf.getType(), "elasticsearch6 type of type is required");

        if (esConf.getUsername() != null) {
            Preconditions.checkNotNull(
                    esConf.getPassword(), "When set the username option, password is required");
        }
    }
}
