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

package com.dtstack.chunjun.connector.solr;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.solr.converter.SolrRawTypeMapper;
import com.dtstack.chunjun.connector.solr.converter.SolrSqlConverter;
import com.dtstack.chunjun.connector.solr.converter.SolrSyncConverter;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

public class SolrConverterFactory {

    private final RowType rowType;
    private final List<String> fieldNames;

    public SolrConverterFactory(SolrConfig solrConfig) {
        fieldNames = new ArrayList<>();
        List<TypeConfig> fieldTypes = new ArrayList<>();
        List<FieldConfig> fields = solrConfig.getColumn();
        for (FieldConfig field : fields) {
            fieldNames.add(field.getName());
            fieldTypes.add(field.getType());
        }

        rowType = TableUtil.createRowType(fieldNames, fieldTypes, SolrRawTypeMapper::apply);
    }

    public SolrSqlConverter createRowConverter() {
        return new SolrSqlConverter(rowType, fieldNames.toArray(new String[] {}));
    }

    public SolrSyncConverter createColumnConverter() {
        return new SolrSyncConverter(rowType, fieldNames.toArray(new String[] {}));
    }
}
