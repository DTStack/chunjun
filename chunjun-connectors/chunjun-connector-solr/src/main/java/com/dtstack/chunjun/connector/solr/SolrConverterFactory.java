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
import com.dtstack.chunjun.connector.solr.converter.SolrColumnConverter;
import com.dtstack.chunjun.connector.solr.converter.SolrRawTypeConverter;
import com.dtstack.chunjun.connector.solr.converter.SolrRowConverter;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ada Wong
 * @program chunjun
 * @create 2021/06/28
 */
public class SolrConverterFactory {

    private final RowType rowType;
    private final List<String> fieldNames;
    private final List<String> fieldTypes;

    public SolrConverterFactory(SolrConf solrConf) {
        fieldNames = new ArrayList<>();
        fieldTypes = new ArrayList<>();
        List<FieldConfig> fields = solrConf.getColumn();
        for (FieldConfig field : fields) {
            fieldNames.add(field.getName());
            fieldTypes.add(field.getType());
        }

        rowType = TableUtil.createRowType(fieldNames, fieldTypes, SolrRawTypeConverter::apply);
    }

    public SolrRowConverter createRowConverter() {
        return new SolrRowConverter(rowType, fieldNames.toArray(new String[] {}));
    }

    public SolrColumnConverter createColumnConverter() {
        return new SolrColumnConverter(rowType, fieldNames.toArray(new String[] {}));
    }
}
