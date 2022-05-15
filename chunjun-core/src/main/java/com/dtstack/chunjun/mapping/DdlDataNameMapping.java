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

package com.dtstack.chunjun.mapping;

import com.dtstack.chunjun.cdc.ddl.entity.ColumnData;
import com.dtstack.chunjun.cdc.ddl.entity.ColumnEntity;
import com.dtstack.chunjun.cdc.ddl.entity.DdlData;
import com.dtstack.chunjun.cdc.ddl.entity.Identity;

import org.apache.commons.collections.MapUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class DdlDataNameMapping implements Mapping<DdlData>, Serializable {

    private static final long serialVersionUID = 1L;

    private final NameMappingRule mappingRule;

    public DdlDataNameMapping(NameMappingConf conf) {
        this.mappingRule = new NameMappingRule(conf);
    }

    @Override
    public DdlData map(DdlData value) {

        if (value instanceof Identity) {
            String table = ((Identity) value).getTable();
            String schema = ((Identity) value).getSchema();

            String targetSchema = mappingRule.schemaMapping(schema);
            String targetTable = mappingRule.tableMapping(schema, table);

            ((Identity) value).setSchema(targetSchema);
            ((Identity) value).setTable(targetTable);
        }

        if (value instanceof ColumnData && value instanceof Identity) {
            String table = ((Identity) value).getTable();
            String schema = ((Identity) value).getSchema();
            Map<String, String> mapFields = mappingRule.getMapFields(schema, table);
            List<ColumnEntity> columnEntity = ((ColumnData) value).getColumnEntity();
            if (MapUtils.isNotEmpty(mapFields)) {
                columnEntity.forEach(
                        filed -> {
                            String targetField =
                                    mappingRule.fieldMapping(filed.getName(), mapFields);
                            filed.setName(targetField);
                        });
            }
        }
        return value;
    }
}
