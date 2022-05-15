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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.solr.SolrConf;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

import java.util.List;

/**
 * @author Ada Wong
 * @program chunjun
 * @create 2021/06/28
 */
public class SolrInputFormatBuilder extends BaseRichInputFormatBuilder<SolrInputFormat> {

    public static SolrInputFormatBuilder newBuild(SolrConf solrConf) {
        List<FieldConf> fields = solrConf.getColumn();
        String[] fieldNames = new String[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            fieldNames[i] = fields.get(i).getName();
        }
        SolrInputFormat format = new SolrInputFormat(solrConf, fieldNames);
        return new SolrInputFormatBuilder(format, solrConf);
    }

    private SolrInputFormatBuilder(SolrInputFormat solrInputFormat, SolrConf solrConf) {
        super(solrInputFormat);
        setConfig(solrConf);
    }

    @Override
    protected void checkFormat() {}
}
